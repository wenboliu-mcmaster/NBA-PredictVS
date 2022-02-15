# -*- coding: utf8 -*-
from __future__ import unicode_literals
from __future__ import division

import os
import copy
import time
import datetime
import cherrypy
from collections import namedtuple

# IntervalFromPy is there, don't worry
# noinspection PyUnresolvedReferences
from psycopg2.extensions import IntervalFromPy

import synmedplus.database.manager
import synmedplus.database.query
import synmedplus.database.queries
import synmedplus.database.notificationpublisher
import synmedplus.common.websocket
import synmedplus.common.constants
import synmedplus.common.fsm
import synmedplus.common.xmllog
import synmedplus.common.galilcom
import synmedplus.common.galilactioncmd
import synmedplus.common.utils
import synmedplus.common.decorators
import synmedplus.common.websocket_broadcast as broadcast
import synmedplus.common.exceptions
import synmedplus.services.production.operationstatus
import synmedplus.services.production.tray_down


FSM_EVT_LAST_OPR = 32
SILENT = 64
TIMING_REFILL = 128

PRODUCTION_STATS_START = 'production_start'
PRODUCTION_STATS_END = 'production_end'
PRODUCTION_STATS_TIME = 'production_time'
PRODUCTION_STATS_ALARM = 'alarm'
PRODUCTION_STATS_REFILL = 'refill'
PRODUCTION_STATS_CHANGETRAY = 'changetray'
PRODUCTION_STATS_IDLE = 'idle'

WATCHDOG_TRAY_ALARM = 'plateau=@IN[6];plateau=?;ala=?'
WATCHDOG_ALARM = 'ala=?'
WATCHDOG_TRAY_ALARM_EMERGENCY = 'emergenc=@IN[7];plateau=@IN[6];plateau=?;ala=?;emergenc=?'
WATCHDOG_ALARM_EMERGENCY = 'emergenc=@IN[7];ala=?;emergenc=?'

MissingContainer = namedtuple('MissingContainer', ['drug_id', 'drug_container_id', 'drug_name', 'is_actionable'])


class PickupDepositCmd(synmedplus.common.galilactioncmd.GalilActionCmd):
    """
    Used to issue pickup/deposit commands to picking units
    """

    def __init__(self, controller, znbf, parentfsm):
        super(PickupDepositCmd, self).__init__(controller, parentfsm)
        self.znbf = znbf
        self.skip_action_on_timeout = True
        self.chksum_vars = {'com', 'syn_id', 'znbf', 'ranbf', 'colbf', 'tagg', 'gc', 'tg',
                            'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'carte', 'per',
                            'tgl', 'frg', 'mass', 'format', 'lect', 'vrfid'}

    def ST_CheckIsReady(self, ctx):
        """
        Overriding base state in order to query tray input
        """
        # if self.timeout_timer.is_ready():
        # Been waiting a long time for galil to be ready
        #    print 'timeout triggered!!!!!'
        if not self.throttle.is_ready():
            return

        msg = 'dempaus=?;pausereq=?;ala=?;antcp=?;tray=@IN[6];tray=?;urgence=@IN[7];urgence=?'

        res = self.set_cmd_and_handle_result(msg)

        if res in synmedplus.common.galilactioncmd.CMD_RESULT_CODES:
            return

        self.throttle.reset()

        try:
            dempaus, pausereq, ala, self.antcp, tray_input, urgence = synmedplus.common.utils.process_galil_response(res)
        except ValueError as expt:
            self.handle_failed_parsing(msg, res, expt)

        if tray_input != synmedplus.common.constants.GALIL_TRAY_INSERTED:
            self.completion_code |= synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_LOST_TRAY
            self.change_state('ST_Ending', 'Someone stole our tray, stopping')
            return

        self.handle_pause_and_emergency(dempaus, pausereq, urgence)

        if self.bmask & synmedplus.common.galilactioncmd.EMERGENCY:
            self.completion_code |= synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_EMERGENCY_STOP
            self.change_state('ST_Ending', 'Ended command early because of emergency stop and buffer validation')
            return

        # We stay in this state until pausereq becomes '0' or we detect alarms
        if dempaus == synmedplus.common.constants.GALIL_PAUSED_FALSE \
                and pausereq == synmedplus.common.constants.GALIL_PAUSE_REQUESTED_FALSE \
                and ala == synmedplus.common.constants.GALIL_NO_ALARM\
                and urgence == synmedplus.common.constants.GALIL_EMERGENCY_STOP_INACTIVE:
            self.change_state('ST_DoAction', 'Everything is good, send command')
            return

        if ala != synmedplus.common.constants.GALIL_NO_ALARM:
            self.change_state('ST_Alarm', ' Detected an alarm before sending the command, querying alarm vector')

    def ST_ResetVars(self, ctx):
        """
        This state is used to make sure variables used in pickup commands are properly reset
        """
        msg = ''
        # VsomPg, VsomDp
        # for i in range(1,9):
        #    msg += 'VsomPg[{i}]=0;VsomDp[{i}]=0;'.format(i=str(i))

        # msg += 'rfid_lu=0;serie_lu=0;gc_lu=0;syn_idf=0;'

        # remove trailing ;
        msg = msg[:-1] if msg[-1] == ';' else msg
        res = self.set_cmd_and_handle_result(msg)

        if res in synmedplus.common.galilactioncmd.CMD_RESULT_CODES:
            return

        self.change_state('ST_DoAction', 'Made sure variables are properly reset')

    def TrOut_DoAction(self, ctx):
        pause_after_send = synmedplus.common.utils.get_config_value('timer', 'picking_unit_pause_after_send')
        if pause_after_send > 0:
            self.log_to_current_state(
                'Taking a {t}s nap before asking for command completion'.format(t=str(pause_after_send)))
            time.sleep(pause_after_send)


class PickingCtx(synmedplus.common.fsm.FsmContext):
    """
    We attempt to decouple an FSM from it's operating data/context
    """
    # This list represents all the states where we are unable to send commands to the robot
    # When Stopping, we will not try to bring tray down if we are coming from one of these states
    CANNOT_CHANGE_TRAY = ['ST_WaitToStart',
                          'ST_Timeout',
                          'ST_EstablishDatabaseConnection',
                          'ST_TestRobotConnection',
                          'ST_PromptScanTrayUI',
                          'ST_TestHomingState',
                          'ST_SocketException',
                          'ST_TestGalilVersion']

    def __init__(self, head_number):
        super(PickingCtx, self).__init__()
        self.timer_connection =  synmedplus.common.utils.ConfigTimer(config_key='connection_retry_throttle',
                                                                    auto=True,
                                                                    init=False)
        self.timer_refresh = synmedplus.common.utils.ConfigTimer(config_key='rfid_refresh_throttle')
        self.throttle_no_current_action = synmedplus.common.utils.ConfigTimer(config_key='get_next_throttle_pu',
                                                                              init=False, auto=False)
        self.throttle_homing_state = synmedplus.common.utils.ConfigTimer(config_key='homing_state_throttle', init=False,
                                                                         auto=False)
        # Initialized when first entering get_next case no_ops (-13)
        # and reset once we leave GetPendingPrescription state
        self.timer_no_ops = None
        self.repeat_query_throttle = synmedplus.common.utils.ConfigTimer(config_key='repeat_query_throttle',
                                                                         init=False,
                                                                         auto=True)
        self.timer_watchdog = synmedplus.common.utils.ConfigTimer(config_key='watchdog_timer', init=True)

        self.head_number = head_number
        self.default_galil_format = None
        self.galil_version = None


        self.active_cmd = None
        self.al = None

        self.remove_tray_reason_id = None
        self.next_operation = None
        self.opr_history = []
        self.current_operation = None
        self.input_tray_id = None
        self.input_tray_serial = None
        self.input_galil_format = None
        self.machine_serial = None

        self.action_sent = False

        self.po_file_id = None
        self.tray_id = None
        self.tray_code = None
        self.po_file_name = None

        self.user_id = 1
        self.prev_tray_id = None
        self.al_pickupdeposit = False

        self.user_stop_flag = False
        self.get_next_status = None
        self.skipped_drugs = set([])
        self.time_stats = {
            'category': None,
            'po_file_header_id': None,
            'time': None,
            'count': 0
        }
        self.tray_progress = None

    def reset(self):
        self.reset_actual_tray_id()
        self.reset_tray_id()
        # Make sure we dont keep an old get_next_status value
        self.get_next_status = None
        self.prev_tray_id = None
        self.al_pickupdeposit = False
        self.user_stop_flag = False
        self.action_sent = False

    def set_tray_id(self, tray_id):
        self.tray_id = tray_id
        self.tray_code = self.get_tray_code_from_tray_id(tray_id)
        self.po_file_id = self.get_po_file_id_from_tray(tray_id)
        self.po_file_name = self.get_po_file_name(self.po_file_id)
        self.reset_skipped_drugs()
        self.tray_progress = None

    def reset_tray_id(self):
        self.tray_id = None
        self.tray_code = None
        self.po_file_id = None
        self.po_file_name = None
        self.reset_skipped_drugs()
        self.tray_progress = None

    def get_machine_serial(self):
        # Get Machine serial
        sql_query = synmedplus.database.queries.get_parameter_value('EXPLOITATION_SITE_SYNMED_SERIAL_NUMBER')
        self.execute_query(sql_query)
        try:
            self.machine_serial = sql_query.get_result()[0]['parameter_value']
            self.machine_serial = int(self.machine_serial)
        except TypeError as expt:
            raise Exception('Parameter EXPLOITATION_SITE_SYNMED_SERIAL_NUMBER not found, make sure the database is properly updated.')

    def get_previous_operation(self):
        try:
            return self.opr_history[-2]
        except IndexError:
            return None

    def get_po_file_id_from_tray(self, tray_id):

        query = synmedplus.database.query.Query("""select a.po_file_header_id 
                                                   from t_po_file_header as a inner join t_tray as b on a.po_file_header_id = b.po_file_header_id
                                                   where b.tray_id = %(t)s""", {'t': tray_id})
        self.execute_query(query)

        res = query.get_result()
        if res:
            return res[0]['po_file_header_id']
        else:
            return 0

    def get_tray_code_from_tray_id(self, tray_id):

        query = synmedplus.database.query.Query("""select tray_code
                                                   from public.t_tray
                                                   where tray_id = %(t)s""", {'t': tray_id})
        self.execute_query(query)

        res = query.get_result()
        if res:
            return res[0]['tray_code']
        else:
            return 0

    def get_next_operation(self):
        query = synmedplus.database.query.Query("SELECT * FROM opr.opr_get_next_oeration(%(po_file)s, %(tray)s)",
                                                {'po_file': self.po_file_id, 'tray': self.tray_id})

        self.execute_query(query)

        # __DB_CHRON__
        if synmedplus.common.utils.get_config_value('global',
                                                    'monitor_db_chron') == synmedplus.common.constants.CONFIG_TRUE:
            q = synmedplus.database.queries.get_db_chron_query('OPR_GET_NEXT_OERATION')
            self.execute_query(q)
            synmedplus.common.utils.log_db_chron_differential(q.get_result(), query.get_query_time())

            q = synmedplus.database.queries.get_db_chron_query('GET_NEXT_HEAD_OPERATION_SYNMED_PLUS')
            self.execute_query(q)
            synmedplus.common.utils.log_db_chron_differential(q.get_result(), query.get_query_time())

        return synmedplus.services.production.operationstatus.TypeProductionOperation(query.get_result()[0])

    def get_next_operation_status(self):
        return self.get_next_operation().return_status

    def add_to_operation_history(self, latest_operation):
        self.opr_history.append(latest_operation)

        if len(self.opr_history) > 5:
            self.opr_history.pop(0)

    def set_production_error(self, po_file_header_id, user_id, po_file_operation_id, valve_id,
                             po_file_operation_detail_id, card_id, error_code):
        query = synmedplus.database.query.Query(
            """SELECT * from opr.opr_set_production_error(
            %(po_file_id)s,
            %(user_id)s,
            %(po_file_op_id)s,
            %(valve_id)s,
            %(po_file_operation_detail_id)s,
            %(card_id)s,
            %(error_code)s
            )""",
            {
                'po_file_id': po_file_header_id,
                'user_id': user_id,
                'po_file_op_id': po_file_operation_id,
                'valve_id': valve_id,
                'po_file_operation_detail_id': po_file_operation_detail_id,
                'card_id': card_id,
                'error_code': error_code
            })

        self.execute_query(query)

        # __DB_CHRON__
        if synmedplus.common.utils.get_config_value('global',
                                                    'monitor_db_chron') == synmedplus.common.constants.CONFIG_TRUE:
            q = synmedplus.database.queries.get_db_chron_query('OPR_SET_PRODUCTION_ERROR')
            self.execute_query(q)
            synmedplus.common.utils.log_db_chron_differential(q.get_result(), query.get_query_time())

        return query.get_result()[0]['opr_set_production_error']

    def set_po_file_operation_done(self, obj_operation_done):
        query_string = "SELECT * from opr.opr_set_oeration_done(%s)" % (obj_operation_done.to_db_params())
        query = synmedplus.database.query.Query(query_string, query_parameters={})
        self.execute_query(query)

        # __DB_CHRON__
        if synmedplus.common.utils.get_config_value('global',
                                                    'monitor_db_chron') == synmedplus.common.constants.CONFIG_TRUE:
            q = synmedplus.database.queries.get_db_chron_query('OPR_SET_OERATION_DONE')
            self.execute_query(q)
            synmedplus.common.utils.log_db_chron_differential(q.get_result(), query.get_query_time())

        return query.get_result()[0]['opr_set_oeration_done']

    def get_galil_format_for_tray_id(self, tray_id):
        po_file_id = self.get_po_file_id_from_tray(tray_id)
        query = synmedplus.database.queries.get_parameter_value('GALIL_COMMAND_FORMAT_VALUE', po_file_id=po_file_id)
        self.execute_query(query)
        return query.get_result()[0]['parameter_value']

    def get_ask_to_start_parameter(self):
        query = synmedplus.database.queries.get_parameter_value(synmedplus.common.constants.DB_ASK_TO_START)
        self.execute_query(query)
        return query.get_result()[0]['parameter_value'] == synmedplus.common.constants.DB_TRUE

    def get_is_hybrid_production_flag(self):
        query = synmedplus.database.queries.get_parameter_value('ALLOW_HYBRID_PRODUCTION')
        self.execute_query(query)
        return query.get_result()[0]['parameter_value'] == synmedplus.common.constants.DB_TRUE

    def set_po_file_status(self, po_file_id, status):

        if po_file_id is not None:
            query = synmedplus.database.query.Query("SELECT * from fpo.fpo_set_po_file_status(%(po_file_id)s, %(status)s)",
                                                    {'po_file_id': po_file_id, 'status': status})
            self.execute_query(query)
            return query.get_result()[0]['fpo_set_po_file_status'] if query.has_result() else 0

        else:
            return 0

    def get_po_file_status(self, po_file_id):
        query = synmedplus.database.query.Query(
            "SELECT po_file_status from t_po_file_header where po_file_header_id = %(id)s", {'id': po_file_id})
        self.execute_query(query)
        return query.get_result()[0]['po_file_status'] if query.has_result() else "0"

    def set_tray_status(self, status):
        return "0"

    def validate_tray_id(self, tray_id, galil_format=None, synmed_serial=''):

        # Validate if the tray_id is one of galil's invalid value
        if tray_id == synmedplus.common.constants.GALIL_RFID_INVALID:
            return synmedplus.common.constants.PU_PRE_VALIDATION_RFID_INVALID_TRAY

        if tray_id == synmedplus.common.constants.GALIL_RFID_NOTHING:
            return synmedplus.common.constants.PU_PRE_VALIDATION_RFID_NO_TRAY

        # Validate if serial number is one of galil's invalid value
        # synmed_serial is none when we validate a scanned tray
        if synmed_serial != '' and synmed_serial == synmedplus.common.constants.GALIL_RFID_INVALID:
            return synmedplus.common.constants.PU_PRE_VALIDATION_RFID_INVALID_SERIAL

        if synmed_serial != '' and synmed_serial == synmedplus.common.constants.GALIL_RFID_NOTHING:
            return synmedplus.common.constants.PU_PRE_VALIDATION_RFID_NO_SERIAL

        # Validate if the scanned tray is associated to a po_file
        po_file = self.get_po_file_id_from_tray(tray_id)
        if po_file == 0:
            return synmedplus.common.constants.PU_PRE_VALIDATION_NO_PO_FILE

        # Validate if the file has already been produced
        if self.get_po_file_status(po_file) == synmedplus.common.constants.SYNSOFT_PO_FILE_STATUS_PRODUCED:
            return synmedplus.common.constants.PU_PRE_VALIDATION_PO_FILE_PRODUCED

        # Validate if the tray has already been produced
        sql_query = synmedplus.database.query.Query(""" SELECT public.iif(count(*) = 0, 'Y','N') as tray_completed
                        from   public.t_po_file_operation as a inner join public.t_card as b on a.card_id = b.card_id
                        where  a.operation_done in ('N','V','I')
                        and    b.tray_id = %s""" % (tray_id))
        self.execute_query(sql_query)
        if sql_query.get_result()[0]['tray_completed'] == synmedplus.common.constants.DB_TRUE:
            return synmedplus.common.constants.PU_PRE_VALIDATION_TRAY_PRODUCED

        # Validate if the tray is being produced (or it is assigned elsewhere)
        sql_query = synmedplus.database.query.Query("""SELECT public.iif(count(*) = 0, 'N', 'Y') as being_produced
                                     from   v_working_area
                                     where  working_area_actual_tray_id = %(tray_id)s""",
                                                    {'tray_id': tray_id})
        self.execute_query(sql_query)
        if sql_query.get_result()[0]['being_produced'] == synmedplus.common.constants.DB_TRUE:
            return synmedplus.common.constants.PU_PRE_VALIDATION_TRAY_PRODUCING

        # Match machine serial number if we got the value from rfid
        # Not if we scanned a tray manually
        if synmed_serial is not None and self.machine_serial != synmed_serial:
            return synmedplus.common.constants.PU_PRE_VALIDATION_SERIAL_MISMATCH

        # Get default galil format
        self.default_galil_format = self.get_default_galil_format()

        # When validating a tray from rfid tag or if hybrid production is off, we dont have a galil_format
        # Format should already have been validated by SMA before burning the tag
        if galil_format is None:
            self.input_galil_format = self.get_galil_format_for_tray_id(tray_id)

        is_compatible = True

        # Test galil support for this format
        if self.get_is_hybrid_production_flag():
            must_validate_galil_format = True

            if self.galil_version['mask'] & synmedplus.common.constants.FIRMWARE_ENABLED_FORMAT:
                is_compatible = True
            else:
                # "INCOMPATIBLE PROD: HYBRID is enabled, without FIRMWARE_ENABLED_FORMAT"
                is_compatible = False

        else:
            # Since hybrid production isn't active, we dont validate galil_format since it is not supplied by UI.
            must_validate_galil_format = False

            if galil_format is None:
                is_compatible = True

            elif galil_format == self.default_galil_format:
                if self.galil_version['mask'] & synmedplus.common.constants.FIRMWARE_ENABLED_FORMAT:
                    is_compatible = True
                elif galil_format != "1":
                    # "INCOMPATIBLE PROD: Galil format !=1 and missing format command for this version"
                    is_compatible = False
            else:
                # Hybrid production is not activated but galil format is not the default one
                is_compatible = False

        # Test if the scanned galil format matches the expected format for this po_file
        if must_validate_galil_format:
            sql_query = synmedplus.database.queries.get_parameter_value('GALIL_COMMAND_FORMAT_VALUE', po_file_id=po_file)

            self.execute_query(sql_query)
            if sql_query.get_result()[0]['parameter_value'] != galil_format:
                is_compatible = False

        if not is_compatible:
            return synmedplus.common.constants.PU_PRE_VALIDATION_INCOMPATIBLE_FORMAT

        # Make sure this is the last validation, other we might bypass other validations
        # if the tray has not been completed by SMA and user decides to continue anyway
        if self.get_manage_with_synmed_assist_parameter(po_file):
            # Validate if the tray has been completed
            sql_query = synmedplus.database.query.Query("""select public.iif(count(a.card_id) = 0,'Y','N') as synmed_assist_completed
                           from   public.t_po_file_drug_exception as a  inner join public.t_card as b
                           on     a.card_id                      = b.card_id
                           where  a.drug_exception_delivery_done = 'N'
                           and    b.tray_id                      = %s""" % (tray_id))
            self.execute_query(sql_query)
            if sql_query.get_result()[0]['synmed_assist_completed'] == synmedplus.common.constants.DB_FALSE:
                return synmedplus.common.constants.PU_PRE_VALIDATION_SMA_INCOMPLETE

        # Valid
        return synmedplus.common.constants.PU_GET_NEXT_RETURN_OK

    def get_production_start_parameter(self):
        parameter_name = 'IS_PICKING_UNIT_{num}_TRAY_RFID_ACTIVE'.format(num=self.head_number)
        query = synmedplus.database.queries.get_parameter_value(parameter_name)
        self.execute_query(query)
        if query.get_result()[0]['parameter_value'] == synmedplus.common.constants.DB_TRUE:
            return synmedplus.common.constants.PU_START_PARAMETER_AUTO
        else:
            return synmedplus.common.constants.PU_START_PARAMETER_MANUAL

    def get_manage_with_synmed_assist_parameter(self, po_file):
        query = synmedplus.database.queries.get_parameter_value('MANAGE_DRUG_EXCEPTION_WITH_SYNMED_ASSIST',
                                                                po_file_id=po_file)
        self.execute_query(query)
        return query.get_result()[0]['parameter_value'] == synmedplus.common.constants.DB_TRUE

    def get_default_galil_format(self):
        query = synmedplus.database.query.Query(
            """select * from fpo.get_profile_parameter_value(
               'GALIL_COMMAND_FORMAT_VALUE',
               (select * from fpo.get_system_default_record_type_card_int()),
               NULL)""")

        self.execute_query(query)
        return query.get_result()[0]['parameter_value'] if query.has_result() else None

    def get_po_file_name(self, po_file_id):
        query = synmedplus.database.query.Query(
            "SELECT po_file_name FROM t_po_file_header where po_file_header_id = %s" % (po_file_id))
        self.execute_query(query)

        return query.get_result()[0]['po_file_name'] if query.has_result() else None

    def log_timeout(self, operation_to_log='BOTH'):
        if operation_to_log in ('CURRENT', 'BOTH'):
            self.current_operation.set(synmedplus.common.constants.DB_TRUE)
            self.current_operation.log_to_db(self, self.current_operation.bitmask_todo,
                                             "PRODUCTION_CARD_ERROR_RS232_COMM_TIMEOUT")
        # also log on previous command with different operation id
        if operation_to_log in ('PREVIOUS', 'BOTH'):
            previous_operation = self.get_previous_operation()
            if previous_operation is not None:
                # Copy the previous_operation so we dont modify it directly
                previous_operation_copy = copy.deepcopy(previous_operation)
                # Force line order id to 0, otherwise the call will not work
                previous_operation_copy.operation_done.po_file_operation_line_order_id = 0
                previous_operation_copy.set(synmedplus.common.constants.DB_TRUE)
                previous_operation_copy.log_to_db(self, previous_operation_copy.bitmask_todo,
                                                  "PRODUCTION_CARD_ERROR_RS232_COMM_TIMEOUT")

    def log_alarms_to_db(self, operation, alvect, antcp=synmedplus.common.constants.GALIL_ANTICIPATION_INACTIVE):

        # iter over alarm vector
        for al in alvect:
            if al > synmedplus.common.constants.MAX_ALARMS:
                al = 0
            db_error_string = synmedplus.common.constants.SYNSOFT_GENERIC_ALARM_STR + "_%03d" % al
            operation.log_to_db(self, operation.bitmask_todo, db_error_string, antcp)

    def get_drug_id_data(self, drug_id):
        query = synmedplus.database.query.Query("SELECT * from t_drug where drug_id = %(drug_id)s",
                                                {'drug_id': drug_id})
        self.execute_query(query)
        return query.get_result()[0] if query.has_result() else None

    def set_actual_tray_id(self, tray_id, po_file_id):

        sql_query = synmedplus.database.query.Query(
            'SELECT * FROM opr.opr_set_actual_tray_id(%(head)s, %(tray_id)s, %(po_file_id)s)',
            {'head': self.head_number, 'tray_id': tray_id, 'po_file_id': po_file_id})
        self.execute_query(sql_query)
        return sql_query.get_result()[0]['opr_set_actual_tray_id'] if sql_query.has_result() else None

    def reset_actual_tray_id(self):
        """
        This function is used to reset the tray of a working area and the active container on a tray
        It is called when changing_tray or stopping production.
        """
        res = ""
        ret = ""
        if self.tray_id is not None:
            sql_query = synmedplus.database.query.Query(
                'SELECT * FROM opr.opr_set_container_in_use(%(head)s,%(tray)s,NULL)',
                {'head': self.head_number, 'tray': self.tray_id})
            self.execute_query(sql_query)
            ret = sql_query.get_result()[0]['opr_set_container_in_use'] if sql_query.has_result() else None

        res = self.set_actual_tray_id(None, None)

        return res, ret

    def get_actual_container_id(self, x, y):
        sql_query = synmedplus.database.query.Query("""
                        select a.working_area_position_actual_drug_container_id as drug_container_id
                        from   public.t_working_area as a
                        where  a.working_area_number     = %(head)s
                        and    a.working_area_position_x = %(x)s
                        and    a.working_area_position_y = %(y)s""",
                                                    {'head': self.head_number, 'x': x, 'y': y})
        self.execute_query(sql_query)
        return sql_query.get_result()[0]['drug_container_id'] if sql_query.has_result() else -1

    def get_missing_containers(self):
        sql_query = synmedplus.database.query.Query(
            """
            select distinct a.drug_id, 
                            c.drug_container_id, 
                            drg.drg_get_display_name(NULL, NULL, a.drug_id) as drug_name, 
                            public.iif(COALESCE(d.working_area_number, 3) = 3, 'Y', 'N') as actionable 
            from   public.t_po_file_operation as a
            inner  join public.t_card as b on a.card_id = b.card_id
            inner  join public.t_drug_container as c on a.drug_id = c.drug_id
            left  join  public.t_working_area as d on c.drug_container_id = d.working_area_position_actual_drug_container_id
            where  a.operation_done in ('N','V','I')
            and    b.tray_id = %(tray_id)s """,
            {
                'tray_id': self.tray_id
            }
        )
        self.execute_query(sql_query)
        return [
            MissingContainer(
                row['drug_id'],
                row['drug_container_id'],
                row['drug_name'],
                row['actionable'] == synmedplus.common.constants.DB_TRUE
            )
            for row
            in sql_query.get_result()
        ]

    def should_broadcast_missing_containers(self, missing_containers, production_progress, warning_threshold):
        actionable_count = 0
        for missing_container in missing_containers:
            if missing_container.is_actionable:
                actionable_count += 1

        at_least_one_container_actionable = actionable_count > 0
        all_containers_actionable = actionable_count == len(missing_containers)
        progression_over_threshold = production_progress >= warning_threshold

        if  all_containers_actionable or (at_least_one_container_actionable and progression_over_threshold) :
            return True
        else:
            return False

    def get_required_external_containers(self, tray_id):

        sql_query = synmedplus.database.query.Query("""
select distinct a.drug_id, c.drug_container_id, drg.drg_get_display_name(NULL, NULL, a.drug_id) as drug_name
from   public.t_po_file_operation as a                                            inner join public.t_card as b
on     a.card_id              = b.card_id                                         inner join public.t_drug_container as c
on     a.drug_id              = c.drug_id                                         left  join public.t_working_area as d
on     c.drug_container_id    = d.working_area_position_actual_drug_container_id
and    d.working_area_number in (1,2)
where  c.drug_container_id   >= 1000
and    b.tray_id              = %(tray_id)s
and    a.operation_done      in ('N','V','I')
and    d.working_area_number is null;""",
                                                    {'tray_id': self.tray_id})
        self.execute_query(sql_query)
        return [(row['drug_container_id'], row['drug_name']) for row in sql_query.get_result()]

    def get_v_no_active_container(self):
        query = synmedplus.database.query.Query("Select * from v_no_active_container")
        self.execute_query(query)

    def set_shake_needed(self, container_id):
        sql_query = synmedplus.database.query.Query(
            "SELECT * from opr.opr_set_container_need_to_be_shaken(%(buffer)s,%(container)s,'N')",
            {'buffer': self.head_number, 'container': container_id})
        self.execute_query(sql_query)
        return sql_query.get_result()[0]['opr_set_container_need_to_be_shaken'] if sql_query.has_result() else None

    def flag_buffer_error(self, position_x, error_id=3):
        sql_query = synmedplus.database.query.Query("""update t_working_area
                                     set   working_area_position_actual_error_id = %(error)s,
                                           working_area_position_actual_drug_container_id = 0
                                     where working_area_number     = %(buffer)s
                                     and   working_area_position_x = %(x)s
                                     and   working_area_position_y = 1""",
                                                    {'buffer': self.head_number, 'x': position_x, 'error': error_id})
        self.execute_query(sql_query)

    def on_tray_finish(self, tray_id, po_file_name, po_file_id):
        publisher = synmedplus.database.notificationpublisher.TrayFinishedNotificationPublisher(po_file_id, tray_id)
        publisher.publish(self.execute_query)

    def on_file_finish(self, po_file_name, po_file_id):
        self.set_po_file_status(po_file_id, synmedplus.common.constants.SYNSOFT_PO_FILE_STATUS_PRODUCED)
        self._update_production_time_stats(PRODUCTION_STATS_END, po_file_id)
        self._update_user_production_stats(po_file_id)
        publisher = synmedplus.database.notificationpublisher.FileFinishedNotificationPublisher(
            po_file_id, po_file_name
        )
        publisher.publish(self.execute_query)

    def on_database_connected(self):
        super(PickingCtx, self).on_database_connected()
        self.get_machine_serial()
        self.reset_actual_tray_id()

    def db_skip_drug(self, user_id, po_file_id, tray_id, drug_id):
        query = synmedplus.database.query.Query(
            """SELECT * FROM opr.opr_skip_drug(%(u)s, %(po)s, %(t)s, %(d)s)""",
            {'u': user_id, 'po': po_file_id, 't': tray_id, 'd': drug_id}
        )
        self.execute_query(query)

    def skip_drug(self, drug_id):
        len_before = len(self.skipped_drugs)
        self.skipped_drugs.add(drug_id)
        len_after = len(self.skipped_drugs)

        if len_after > len_before:
            self.db_skip_drug(self.user_id, self.po_file_id, self.tray_id, drug_id)

    def reset_skipped_drugs(self):
        self.skipped_drugs = set([])

    def set_production_user_id(self, po_file_id, user_id):
        query = synmedplus.database.query.Query("""select * from fpo.fpo_set_po_header_value((
            %(po_file_header_id)s,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            %(production_uid)s,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL));""", {'po_file_header_id': po_file_id, 'production_uid': user_id})
        self.execute_query(query)
        return query.get_result()

    def _update_production_time_stats(self, category=None, po_file_header_id=0, time=0, count=0):

        queries = []
        time_interval = datetime.timedelta(seconds=time)
        if category == PRODUCTION_STATS_START:
            queries.append(self._update_production_time_start(po_file_header_id))

        elif category == PRODUCTION_STATS_END:
            queries.append(self._update_production_time_end(po_file_header_id))

        elif category == PRODUCTION_STATS_TIME:
            queries.append(self._update_production_time(po_file_header_id, time_interval))
            queries.append(self._update_head_working_time_production(time_interval))

        elif category == PRODUCTION_STATS_ALARM:
            if po_file_header_id:
                queries.append(self._update_alarm_downtime(po_file_header_id, time_interval, count))
            queries.append(self._update_head_working_time_alarm(time_interval))

        elif category == PRODUCTION_STATS_REFILL:
            queries.append(self._update_refill_downtime(po_file_header_id, time_interval, count))
            queries.append(self._update_head_working_time_waiting(time_interval))

        elif category == PRODUCTION_STATS_CHANGETRAY:
            queries.append(self._update_changetray_downtime(po_file_header_id, time_interval))
            queries.append(self._update_head_working_time_waiting(time_interval))

        elif category == PRODUCTION_STATS_IDLE:
            queries.append(self._update_head_working_time_idle(time_interval))

        for q in queries:
            self.execute_query(q)

    def _update_production_time(self, po_file_header_id, production_time):
        return synmedplus.database.query.Query(
            """select * from fpo.set_po_file_production_stats(%(id)s, NULL, NULL, %(p_time)s, NULL, NULL, NULL);""",
            {'id': po_file_header_id,
             'p_time': IntervalFromPy(production_time)
             })

    def _update_production_time_start(self, po_file_header_id):
        return synmedplus.database.query.Query(
            """select * from fpo.set_po_file_production_stats(%(id)s, NULL, NULL, '00:00:00', NULL, NULL, NULL);""",
            {'id': po_file_header_id})

    def _update_production_time_end(self, po_file_header_id):
        """
        Must be called only after po file status has been set to completed
        """
        return synmedplus.database.query.Query(
            """select * from fpo.set_po_file_production_stats(%(id)s, NULL, NULL, NULL, NULL, NULL, NULL);""",
            {'id': po_file_header_id})

    def _update_alarm_downtime(self, po_file_header_id, downtime, count):
        return synmedplus.database.query.Query(
            """select * from fpo.set_po_file_production_stats(%(id)s, %(count)s, NULL, NULL, %(alarm_time)s, NULL, NULL); """,
            {'id': po_file_header_id,
             'count': count,
             'alarm_time': IntervalFromPy(downtime)})

    def _update_refill_downtime(self, po_file_header_id, downtime, count):
        return synmedplus.database.query.Query(
            """select * from fpo.set_po_file_production_stats(%(id)s, NULL, %(count)s, NULL, NULL, %(refill_time)s, NULL); """,
            {'id': po_file_header_id,
             'refill_time': IntervalFromPy(downtime),
             'count': count})

    def _update_changetray_downtime(self, po_file_header_id, downtime):
        return synmedplus.database.query.Query(
            """select * from fpo.set_po_file_production_stats(%(id)s, NULL, NULL, NULL, NULL, NULL, %(changetray_time)s); """,
            {'id': po_file_header_id,
             'changetray_time': IntervalFromPy(downtime)}
        )

    def _update_head_working_time_idle(self, time_interval):

        return synmedplus.database.query.Query(
            """select * from opr.opr_compute_head_working_time(%(head)s,%(time)s,NULL,NULL,NULL)""",
            {'head': -self.head_number,
             #'time': AsIs(synmedplus.common.utils.db_seconds_to_interval(time_interval)
             'time': IntervalFromPy(time_interval)
             })

    def _update_head_working_time_waiting(self, time_interval):
        return synmedplus.database.query.Query(
            """select * from opr.opr_compute_head_working_time(%(head)s,NULL,%(time)s,NULL,NULL)""",
            {'head': -self.head_number,
             'time': IntervalFromPy(time_interval)})

    def _update_head_working_time_alarm(self, time_interval):
        return synmedplus.database.query.Query(
            """select * from opr.opr_compute_head_working_time(%(head)s,NULL,NULL,%(time)s, NULL)""",
            {'head': -self.head_number,
             'time': IntervalFromPy(time_interval)})

    def _update_head_working_time_production(self, time_interval):
        return synmedplus.database.query.Query(
            """select * from opr.opr_compute_head_working_time(%(head)s,NULL,NULL,NULL,%(time)s)""",
            {'head': -self.head_number,
             'time': IntervalFromPy(time_interval)})

    def start_category_timer(self, category, po_file_header_id=0, count=0):
        if self.time_stats['category'] is not None:
            self.time_stats['time'] = time.time() - self.time_stats['time']
            self._update_production_time_stats(**self.time_stats)

        self.time_stats = {'category': category,
                           'po_file_header_id': po_file_header_id,
                           'time': time.time(),
                           'count': count}

    def validate_usable_containers_on_buffer(self, tray_id):
        """
        This function is used to set a specific error on every container we find on this head's buffer.
        Doing so will ensure the transporter re-validates the containers 
        (in the event that we disabled/removed rfid on buffers)
        """
        q = synmedplus.database.query.Query(
            """  update public.t_working_area as source
                    set    working_area_position_actual_error_id = (select a.lookup_value_id 
                                                                    from   public.v_lookup_code as a 
                                                                    where  a.lookup_code  = 'WORKING_AREA_POSITION_ERROR_ID' 
                                                                    and    a.lookup_value = 'MUST_VALIDATE_CONTAINER'
                                                                    )
                    from public.t_po_file_operation as opr 
                      inner join public.t_card as card on opr.card_id = card.card_id
                      inner join public.t_drug_container as container on opr.drug_id = container.drug_id
                    where  source.working_area_number = %(working_area_number)s
                    and    source.working_area_position_actual_drug_container_id = container.drug_container_id  
                    and    card.tray_id = %(tray_id)s
                    and    opr.operation_done in ('N', 'V', 'I');""",
            {'working_area_number': self.head_number,
             'tray_id': tray_id}
        )
        self.execute_query(q)

    def get_completed_operation_count(self, po_file_header_id):
        query = synmedplus.database.query.Query(
            "select count(*) as count from \
            public.t_po_file_operation \
            where po_file_header_id = %(po_file)s and operation_done = 'Y'",
            {'po_file': po_file_header_id}
        )
        self.execute_query(query)

        return query.get_result()[0]['count']

    def _update_user_production_stats(self, po_file_header_id):
        query = synmedplus.database.query.Query(
            "select * from usr.usr_insert_production_stats(%(id)s);",
            {'id': po_file_header_id}
        )
        self.execute_query(query)

class PickingUnit(synmedplus.common.fsm.ServiceFsm):
    """
    Represents a picking unit, which issues commands to a Galil Controller
    """
    STATE_INIT_FIRST = 'ST_EstablishDatabaseConnection'
    STATE_MAIN_FIRST = 'ST_Start'
    STATE_SHUTDOWN_FIRST = 'ST_ResetProductionVariables'

    def __init__(self, head_number, ctx=None, parentfsm=None, logs_dir=None):
        name = '{}{}'.format(synmedplus.common.constants.FSM_PICKING_UNIT_NAME_PREFIX, head_number)
        if ctx is None:
            ctx = PickingCtx(head_number)
        if logs_dir is None:
            # Necessary to write the logs at the appropriate location, relative to this file
            thisdir = os.path.abspath(os.path.dirname(__file__))
            logs_dir = os.path.join(thisdir, synmedplus.common.constants.SERVICE_LOG_DIR)
        super(PickingUnit, self).__init__(name,
                                          ctx,
                                          parentfsm=parentfsm,
                                          logs_folder=logs_dir,
                                          create_query_manager=True)
        cherrypy.engine.subscribe(self.get_name(), self._handle_websocket_message)

        self.ctrl = None
        self.last_broadcast = None
        self.timeout_timer = None
        self.temp_timer = None
        self.temp_times = []
        self.entry_timer = None
        self.alarm_hook_dict = {
            35: 'validate_buffer_hook',
            89: ['disable_buffer_position', 'flag_bad_rfid_tag'],
            90: ['disable_buffer_position', 'flag_bad_rfid_tag'],
            91: 'flag_bad_rfid_tag',
            92: ['flag_bad_rfid_tag'],
            160: ['disable_buffer_position', 'flag_bad_rfid_tag']
        }
        self.time_keeper = None

    def alarm_hook(self, ctx, alarm_list):

        alarm_set = set(alarm_list)
        alarm_set.discard(0)

        for alarm in alarm_set:
            if alarm in self.alarm_hook_dict:
                # alarms can have 1 or more hooks associated with them
                hooks = self.alarm_hook_dict[alarm]
                # Funnel function calls
                if isinstance(hooks, basestring):
                    hooks = [hooks]
                # Try to call all hooks in the list
                if isinstance(hooks, list):
                    for hook in hooks:
                        try:
                            # Get the hook and call it with context
                            getattr(self, hook)(ctx, alarm)
                        except Exception as expt:
                            self.log_to_current_state('Missing alarm hook function %s for %s' % (hook, alarm))
                            self.log_exception(expt)

    def get_loop_sleep(self):
        return self._config.loop_sleep

    def set_user_id(self, msg, user_id):
        super(PickingUnit, self).set_user_id(msg, user_id)
        if self.ctx:
            self.ctx.user_id = user_id

    def flag_bad_rfid_tag(self, ctx, alarm_num):
        self.log_to_current_state(
            'BAD_RFID_TAG: {al},{c}'.format(al=alarm_num, c=ctx.current_operation.operation_done.drug_container_id))

    def disable_buffer_position(self, ctx, alarm_num):
        """
        Hook for alarms 89,90,160
        We flag the location in error and assign the container 0 to it
        """
        ctx.flag_buffer_error(ctx.current_operation.operation_done.working_area_position_x)

    def disable_buffer_position_al_92(self, ctx, alarm_num):
        """
        Hook for alarm 92
        Flags the buffer location with a specific error_id so the transporter will expel the
        current container and deactivate the location
        """
        ctx.flag_buffer_error(ctx.current_operation.operation_done.working_area_position_x, error_id=6)

    def validate_buffer_hook(self, ctx, alarm_num):
        """
        Hook for alarm 35 (emergency stop or doors open)
        Flag every container on buffer with an error which will cause the
        transporter to validate them. 
        """
        ctx.validate_usable_containers_on_buffer(ctx.tray_id)

    def _validate_buffer_check(self, emergency_input):
        if emergency_input == synmedplus.common.constants.GALIL_EMERGENCY_STOP_ACTIVE:
            # Doors were opened, we need to validate every container on the buffer
            self.ctx.validate_usable_containers_on_buffer(self.ctx.tray_id)

    def _init_controller(self):
        if not self.ctrl:
            self.ctrl = synmedplus.common.galilcom.Controller(self.get_name(), self, self._config.host, self._config.port,
                                                              self._config.com_sleep)
            self.ctrl.set_logger_name(self.get_logger_name())

    def destroy_controller(self):
        if not self.ctrl:
            return
        self.ctrl.destroy()
        self.ctrl = None

    def start(self):
        # Thread-safe, using msg Queues()
        self.ctx.log = getattr(self, 'log_to_current_state')
        return super(PickingUnit, self).start()

    def msg_start_picking_unit(self, tray_id, **kwargs):

        # galil_format is now optional if hybrid production is not used.
        try:
            galil_format = str(kwargs['galil_format'])
            if galil_format == '':
                galil_format = None
        except KeyError:
            galil_format = None

        message = synmedplus.common.fsm.DynamicMessage(
            {'tray_id': int(tray_id), 'galil_format': galil_format},
            'tray_information')
        self.add_state_message('ST_PromptScanTrayUI', message)
        return True

    def msg_stop_picking_unit(self, **kwargs):
        self._stop_with_reason(synmedplus.common.constants.STOP_USER_REQUESTED)
        self._msg_queue.put(synmedplus.common.fsm.FSM_STOP)
        return True

    def msg_skip_drugs(self, drugs, **kwargs):
        message = synmedplus.common.fsm.DynamicMessage({'drugs_to_skip': drugs}, 'skip_drugs')
        self.add_state_message('ST_RequestSkipDrugs', message)
        return True

    def handle_cmd_completed(self, cmd, alarm_next_state=''):
        """
        This method was introduced to prevent code duplication when testing
        for a command completion code. It centralises state changes for common
        error cases such as timeouts, socket errors and alarms which are present
        in almost all command FSM sent to galil.
        
        This function only handles error cases. Therefore, success action must 
        be implemented in the calling state. This allows us to catch the return
        value of a query and to handle it.
        
        """
        success = False
        # self.log_to_current_state('error bitmask: %s'%cmd.get_error())
        self.ctx.action_sent = cmd.get_cmd_started()
        if cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM:
            self.ctx.al_next_state = alarm_next_state
            self.ctx.al = cmd.get_alarm()  # Get alarm details which have been queried
            if not synmedplus.common.utils.alarms_known(self.ctx.al):
                self._stop_with_reason(synmedplus.common.constants.STOP_UNKNOWN_ALARM)
                self.change_state('ST_Stopping', 'Galil returned an unknown alarm, stopping')
                return
            self.change_state('ST_AlarmDetected')
        elif cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_TIMEOUT:
            self.change_state('ST_Timeout')
        elif cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_SOCKET_ERROR:
            self.change_state('ST_SocketException', 'Received socket error exception while communicating with galil')
        elif cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM_NONE:
            self.ctx.al = None
            self.log_to_current_state('Attempted to reset alarms but they were already fixed')
            success = True
        elif cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM_NEW:
            self.ctx.al_next_state = alarm_next_state
            self.ctx.al = cmd.get_alarm()
            if not synmedplus.common.utils.alarms_known(self.ctx.al):
                self._stop_with_reason(synmedplus.common.constants.STOP_UNKNOWN_ALARM)
                self.change_state('ST_Stopping', 'Galil returned an unknown alarm, stopping')
                return
            self.change_state('ST_AlarmDetected',
                              'Attempted to reset alarms but alarm vector changed while processing the request, new alvect : %s' % self.ctx.al)
        else:
            success = True

        return success

    def broadcast(self, broadcast_obj):
        """
        Overriding the default broadcast function to initialise to internal timers
        :raises WebsocketBroadcastError: If attempting to broadcast something other than a Broadcast object
        :param broadcast_obj: <Broadcast>, the broadcast to emit
        :return: none
        """
        if isinstance(broadcast_obj, broadcast.AlarmBroadcast):
            self.timeout_timer = synmedplus.common.utils.ConfigTimer(config_key='alarm_reset_timer',
                                                                     auto=True,
                                                                     init=True)
        if isinstance(broadcast_obj, broadcast.RequestBroadcast):
            self.timeout_timer = synmedplus.common.utils.ConfigTimer(config_key='user_request_timer',
                                                                     auto=True,
                                                                     init=True)
        super(PickingUnit, self).broadcast(broadcast_obj)

    def _pre_update(self):
        """
        Called from the same thread as update(), OK to affect FSM here
        """
        if self.should_stop() and not self.ctx.user_stop_flag:
            self.ctx.user_stop_flag = True

            if self.get_statename() not in ['ST_DoPickupDeposit', 'ST_QueryValveStatus', 'ST_AssessPickupDeposit']\
                    or self._critical_stop:
                self.change_state('ST_Stopping', 'Received Stop')
                # Make sure active_cmd is cleared, so we dont use an old command
                self.ctx.active_cmd = None

            if self.get_statename() == 'ST_DoPickupDeposit' and self.ctx.active_cmd and not self.ctx.active_cmd.get_cmd_started():
                # Wanted to start a command, but didnt send it yet, mark it 'N' and stop
                # Allows stopping while emergency or pause is active and we are stuck
                # In PickupDeposit ready check
                self.ctx.current_operation.set(synmedplus.common.constants.DB_FALSE)
                self.ctx.current_operation.log_to_db(self.ctx)
                self.change_state('ST_Stopping', 'Received Stop')
                # Make sure active_cmd is cleared, so we dont use an old command
                self.ctx.active_cmd = None

    def TrIn_WaitToStart(self, ctx):
        ctx.start_category_timer(PRODUCTION_STATS_IDLE)

    def ST_WaitToStart(self, ctx):

        if self.should_start():
            # self.broadcast(broadcast.ComponentStarted())
            self._init_controller()
            self.reset_all_state_messages()
            self.change_state('ST_InitialisationBegin', 'Received Start.')

    def ST_EstablishDatabaseConnection(self, ctx):

        if ctx.timer_connection.is_ready():
            self.dbm.connect()
            if self.dbm.connected():
                ctx.on_database_connected()
                self.broadcast(broadcast.DatabaseConnectionEstablished())
                ctx.timer_connection.init()
                self.change_state('ST_TestDatabaseVersion',
                                  'Connection to database established, testing database version.')

            else:
                self.log_to_current_state('Unable to connect to database, retrying in 10 seconds')
                self.broadcast(broadcast.DatabaseConnectionFailed(retry_delay=ctx.timer_connection.get_delay()))

    def ST_TestDatabaseVersion(self, ctx):

        expected_database_version = synmedplus.common.utils.get_config_value('version', 'database_version')
        self.log_to_current_state('Database version: {}'.format(ctx.database_version))
        self.log_to_current_state('Expected version: {}'.format(expected_database_version))
        self.log_to_current_state('Newer version allowed?: {}'.format(
            synmedplus.common.utils.get_config_value('version', 'allow_newer_versions')
        ))

        if ctx.database_version == expected_database_version:
            self.change_state('ST_TestRobotConnection', 'Versions match exactly, continuing.')

        elif ctx.database_version < expected_database_version:
            self.broadcast(broadcast.DatabaseVersionMismatch())

            self._stop_with_reason(synmedplus.common.constants.STOP_DATABASE_VERSION)
            message = 'Version older than expected, stopping'
            self.change_state('ST_Stopping', message)
            raise Exception(message)

        else:
            self.log_to_current_state('Version is newer than expected.')
            if synmedplus.common.utils.get_config_value('version', 'allow_newer_versions') \
                    == synmedplus.common.constants.CONFIG_TRUE:
                self.change_state('ST_TestRobotConnection', 'Newer versions are allowed, continuing.')
            else:
                self.broadcast(broadcast.DatabaseVersionMismatch())
                self._stop_with_reason(synmedplus.common.constants.STOP_DATABASE_VERSION)
                message = 'Newer versions are not allowed.'
                self.change_state('ST_Stopping', message)
                raise Exception(message)

    def TrIn_TestRobotConnection(self, ctx):
        self.broadcast(broadcast.RobotConnectionAttempt())

    def ST_TestRobotConnection(self, ctx):
        """
        This state is used to test if the FSM can connect to the robot.
        If it can't, It will indefinitely loop in this state and periodically retry to connect
        The reconnect delay is based on a global config parameter
        Once a connection is made, we move on to the next step.
        """
        if ctx.timer_connection.is_ready():
            test_result = synmedplus.common.utils.test_socket_open(self._config.host, self._config.port)
            if test_result == synmedplus.common.constants.OK:
                self.broadcast(broadcast.RobotConnectionEstablished())
                ctx.timer_connection.init()
                self.change_state('ST_TestGalilVersion', 'Successfully connected to robot')
                return
            else:
                msg = 'Unable to connect to %s (%s, %s) is closed, error: %s. Retrying in %s seconds' % (
                    self.get_name(), self._config.host, self._config.port, test_result, ctx.timer_connection.get_delay())
                self.log_to_current_state(msg)
                self.broadcast(broadcast.RobotConnectionFailed(retry_delay=ctx.timer_connection.get_delay()))

    def ST_TestGalilVersion(self, ctx):
        """
        This state is used to query galil for it's version. This value is compared to the minimum
        galil version stored in our config file. If the queried version is lower than the expected
        version, we stop prodution and warn the user 
        """
        if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.QueryGalilVersion):
            if ctx.current_operation is not None:
                # Reset operation status values
                ctx.current_operation.reset()
            ctx.active_cmd = synmedplus.common.galilactioncmd.QueryGalilVersion(self.ctrl, self)
            ctx.active_cmd.send()

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            if self.handle_cmd_completed(ctx.active_cmd):
                ctx.galil_version = ctx.active_cmd.get_result()
                if ctx.galil_version['raw'] < synmedplus.common.utils.get_config_value(
                        'version', 'minimum_picking_unit_version'):
                    # The galil version we queried is lower than the one we expect
                    # stopping with warning
                    self._stop_with_reason(synmedplus.common.constants.STOP_GALIL_VERSION)
                    self.change_state('ST_Stopping', 'Lower than minimal galil version, stopping')
                else:
                    self.change_state('ST_ResetScanP', 'Galil version is fine, making sure scanP variable is in [0,1]')
            ctx.active_cmd = None

    def ST_ResetScanP(self, ctx):
        """
        This state is used to make sure the pickingunit is set to use scanP in (0,1)
        """

        if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.ResetScanP):
            ctx.active_cmd = synmedplus.common.galilactioncmd.ResetScanP(self.ctrl, self)
            ctx.active_cmd.send()

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            if ctx.active_cmd.val in [0, 1]:
                self.change_state('ST_TestHomingState', 'ScanP value is safe, proceeding')
            else:
                # Failed to reset scanP ???
                raise Exception('Failed to reset scanP value!!!')

    def TrIn_TestHomingState(self, ctx):
        self.broadcast(broadcast.WaitingForHoming())

    def ST_TestHomingState(self, ctx):

        if ctx.throttle_homing_state.is_ready():
            if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.QueryHomingState):
                ctx.active_cmd = synmedplus.common.galilactioncmd.QueryHomingState(self.ctrl, self)
                ctx.active_cmd.send()

            ctx.active_cmd.update()

            if ctx.active_cmd.has_completed():
                if self.handle_cmd_completed(ctx.active_cmd):
                    hstate = ctx.active_cmd.get_result()[0]
                    ala = ctx.active_cmd.get_result()[1]
                    alarms = synmedplus.common.utils.filter_alarm_vector(
                        ctx.active_cmd.get_result()[2:]
                    )
                    if ala != 0 and not hstate == synmedplus.common.constants.GALIL_HOMED_STATE:
                        # Homing will never complete because of the alarm, user must restart the machine
                        self._stop_with_reason(synmedplus.common.constants.STOP_ALARM_PREVENTS_HOMING,
                                               {'alarms': list(alarms)})
                        self.change_state('ST_Stopping',
                                          'Alarm detected during homing which will never complete, stopping.')

                    if hstate == synmedplus.common.constants.GALIL_HOMED_STATE:
                        self.change_state(self.STATE_INIT_END, 'Robot homed.')
                ctx.active_cmd = None

    def TrIn_Start(self, ctx):
        ctx.reset()

    def ST_Start(self, ctx):
        """
        This state is used as the real entry point to start the fsm.
        
        Before starting production, we always make sure the tray magnet is freed
        so the user can actually supply us with a tray.
        """
        # Tell ChangeTray that we dont need to request user input
        self.add_state_message('ST_ChangeTray', synmedplus.common.fsm.BitmaskMessage(SILENT))
        # ChangeTray will then go to ST_ChooseTrayInputMethod upon freeing the tray magnet
        self.change_state('ST_ChangeTray', 'Starting, going to change_tray to free tray magnet')

    def ST_ChooseTrayInputMethod(self, ctx):
        """
        This state is used to obtain the value of the ProductionStart parameter from the DB.
        Possible values:
        - Manual: Production is started by manually scanning a tray
        - Auto  : Production is started by placing a tray on a PU, tray id is read by RFID on antenna 14
        """
        if ctx.get_production_start_parameter() == synmedplus.common.constants.PU_START_PARAMETER_AUTO:
            self.change_state('ST_WaitTrayAndScanRfid', 'Start parameter is set to Auto, refreshing tray RFID')
        else:
            self.change_state('ST_PromptScanTrayUI', 'Start parameter is set to Manual, waiting for user to scan tray')

    def TrIn_PromptScanTrayUI(self, ctx):
        # Reset state messages so we only consider responses from this specific request.
        self.reset_state_messages('ST_PromptScanTrayUI')
        self.broadcast(broadcast.PickingUnitRequestTray())

    def WD_PromptScanTrayUI(self, ctx, watchdog_result):
        if watchdog_result[0] != synmedplus.common.constants.GALIL_NO_ALARM:
            ctx.al_next_state = 'ST_Start'
            self.change_state('ST_QueryAlarmVector', 'Watchdog detected alarm(s)')

    @synmedplus.common.decorators.watchdog(WATCHDOG_ALARM, WD_PromptScanTrayUI)
    def ST_PromptScanTrayUI(self, ctx):
        """
        This state is used to wait for user input. We need a tray_id before starting production
        """
        # if self.timeout_timer.is_ready():
        # We have been waiting to a tray to be scanned for a long time
        #    self.log_to_current_state('WAITED WONGGGGGGGGGGGGGGGGGGGG TIME')
        #    pass
        # Loop until we receive the tray_id from ui
        tray_information_message = self._get_dynamic_message('tray_information')

        if tray_information_message is not None:
            ctx.input_tray_id = tray_information_message.get_message()['tray_id']
            ctx.input_galil_format = tray_information_message.get_message()['galil_format']
            self.change_state('ST_ValidateTray',
                              'User scanned a tray: %s, switching to validation state' % ctx.input_tray_id)
            return

    def TrIn_WaitTrayAndScanRfid(self, ctx):
        self.broadcast(broadcast.WaitingForTray())

    def ST_WaitTrayAndScanRfid(self, ctx):
        """
        This state is used to send the tray rfid refresh command.
        This command may complete with alarms,timeout, socket errors
        If successful -> ST_QueryTrayRfidValue
        """

        if ctx.timer_refresh.is_ready():
            if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.WaitUntilTrayCmd):
                ctx.active_cmd = synmedplus.common.galilactioncmd.WaitUntilTrayCmd(self.ctrl, self)
                ctx.active_cmd.send()

            ctx.active_cmd.update()

            if ctx.active_cmd.has_completed():
                if self.handle_cmd_completed(ctx.active_cmd, alarm_next_state='ST_WaitTrayAndScanRfid'):
                    ctx.input_tray_id = ctx.active_cmd.tray_rfid
                    ctx.input_tray_serial = ctx.active_cmd.tray_serial
                    # if ctx.input_tray_id != synmedplus.common.constants.GALIL_RFID_NOTHING or \
                    # ctx.input_tray_serial != synmedplus.common.constants.GALIL_RFID_NOTHING:
                    ctx.input_galil_format = None
                    self.change_state('ST_ValidateTray', 'Successfully obtained tray rfid value')
                ctx.active_cmd = None

    def ST_ValidateTray(self, ctx):
        """
        This state is used to validate the tray id before starting production.

        If the tray is valid, production is started.
        If it is not, we move to a warning state with an explanation on why validation failed 
        then wait until the user removes the tray and sends the ok to restart
        
        """
        # ctx.tray_id
        # send tray id to db for validation
        ctx.tray_validation_result = ctx.validate_tray_id(ctx.input_tray_id, ctx.input_galil_format, ctx.input_tray_serial)
        self.log_to_current_state(
            'Validation result for tray_id(%s): %s' % (ctx.input_tray_id, ctx.tray_validation_result))

        if ctx.tray_validation_result in [synmedplus.common.constants.PU_PRE_VALIDATION_OK,
                                          synmedplus.common.constants.PU_PRE_VALIDATION_SMA_INCOMPLETE]:
            ctx.set_tray_id(ctx.input_tray_id)
            # Set the re-identification flag for every container on this buffer
            # Make sure that subsequent pickups are from known containers, and we dont have
            # any left-over container from previous runs
            ctx.validate_usable_containers_on_buffer(ctx.tray_id)
            ctx.set_actual_tray_id(ctx.tray_id, ctx.po_file_id)

            ctx.start_category_timer(PRODUCTION_STATS_CHANGETRAY, ctx.po_file_id)

        if ctx.tray_validation_result == synmedplus.common.constants.PU_PRE_VALIDATION_OK:
            self.change_state('ST_WaitTrayInserted',
                              'Validated supplied tray id, Making sure the tray is well inserted')
        elif ctx.tray_validation_result == synmedplus.common.constants.PU_PRE_VALIDATION_SMA_INCOMPLETE:
            self.change_state('ST_PromptSynMedAssistAction',
                              'Tray has not been completed by SynMedAssist, asking user input')
        else:
            # remove_tray_reason_id is used in TrIn_PromptRemoveTrayUI
            ctx.remove_tray_reason_id = ctx.tray_validation_result
            self.change_state('ST_ChangeTray', 'Tray id was invalid, warning user and waiting for action')
            return

    def TrIn_PromptSynMedAssistAction(self, ctx):
        self.broadcast(broadcast.PickingUnitRequestStartProductionNoSMA())

    def WD_PromptSynMedAssistAction(self, ctx, watchdog_result):
        if watchdog_result[0] != synmedplus.common.constants.GALIL_NO_ALARM:
            # alarm detected
            ctx.al_next_state = 'ST_Start'
            self.change_state('ST_QueryAlarmVector', 'Watchdog detected alarms')

        self._validate_buffer_check(watchdog_result[1])

    @synmedplus.common.decorators.watchdog(WATCHDOG_ALARM_EMERGENCY, WD_PromptSynMedAssistAction)
    def ST_PromptSynMedAssistAction(self, ctx):
        """
        This state is used to wait until the user decides whether to produce this tray even thought it was not
        finished by SynMedAssist or to stop.
        """

        # if self.timeout_timer.is_ready():
        #    #We have been waiting to a tray to be scanned for a long time
        #    pass
        if self.received_yes():
            self.change_state('ST_WaitTrayInserted',
                              'User decided to continue anyway, making user tray is well inserted')
            return

        if self.received_no():
            ctx.remove_tray_reason_id = synmedplus.common.constants.PU_PRE_VALIDATION_SMA_INCOMPLETE
            self.change_state('ST_ChangeTray', 'Tray was not completed by SMA, user decided to stop')
            return

    def TrIn_WaitTrayInserted(self, ctx):
        self.broadcast(broadcast.WaitingForTray())

    def ST_WaitTrayInserted(self, ctx):
        """
        This state is used to wait until the tray is completely inserted in the machine before
        starting production.
        """
        if ctx.repeat_query_throttle.is_ready():
            if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.QueryTrayInserted):
                ctx.active_cmd = synmedplus.common.galilactioncmd.QueryTrayInserted(self.ctrl, self)
                ctx.active_cmd.send()

            ctx.active_cmd.update()

            if ctx.active_cmd.has_completed():
                if self.handle_cmd_completed(ctx.active_cmd):
                    tray_status = ctx.active_cmd.get_result()[0]
                    if tray_status == synmedplus.common.constants.GALIL_TRAY_INSERTED:
                        self.change_state('ST_WaitTrapDoorClosed',
                                          'Confirmed that tray is well inserted, waiting for trap door to close.')

                ctx.active_cmd = None

    def TrIn_WaitTrapDoorClosed(self, ctx):
        self.broadcast(broadcast.WaitingForTrap())

    def ST_WaitTrapDoorClosed(self, ctx):
        """
        This state is used to wait until the tray is completely inserted in the machine before
        starting production.
        """
        if ctx.repeat_query_throttle.is_ready():
            if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.QueryTrapClosed):
                ctx.active_cmd = synmedplus.common.galilactioncmd.QueryTrapClosed(self.ctrl, self)
                ctx.active_cmd.send()

            ctx.active_cmd.update()

            if ctx.active_cmd.has_completed():
                if self.handle_cmd_completed(ctx.active_cmd):
                    if ctx.active_cmd.get_result() == synmedplus.common.constants.GALIL_TRAP_CLOSED:
                        self.change_state('ST_TestAutoStartParameter',
                                          'Confirmed that trap door is closed, starting production')
                ctx.active_cmd = None

    def ST_TestAutoStartParameter(self, ctx):
        """
        This state is used as a gateway to production start. In some cases, we must wait for user input before starting production.
        We test this here, based on a database parameter.
        If the parameter is active, we transition to a state waiting for user confirmation.
        If the parameter is not active, we transition to StartProduction and continue as usual.
        
        """
        if ctx.get_ask_to_start_parameter():
            self.change_state('ST_PromptUserStartProduction',
                              'Need user confirmation before production starts, switching to user input waiting state')
        else:
            self.change_state('ST_StartProduction', 'User confirmation is not required to start, starting production')

    def TrIn_PromptUserStartProduction(self, ctx):
        self.broadcast(broadcast.PickingUnitRequestStartProduction(ctx.po_file_id, ctx.po_file_name, ctx.tray_id))

    def WD_PromptUserStartProduction(self, ctx, watchdog_result):
        if watchdog_result[0] != synmedplus.common.constants.GALIL_TRAY_INSERTED:
            # User removed tray, no need for him to confirm on the interface
            self.change_state('ST_Start', 'Watchdog detected that the tray has been removed, going back to loop start.')

        elif watchdog_result[1] != synmedplus.common.constants.GALIL_NO_ALARM:
            # Ala is raised
            ctx.al_next_state = 'ST_PromptUserStartProduction'
            self.change_state('ST_QueryAlarmVector', 'Watchdog detected alarms')

        self._validate_buffer_check(watchdog_result[2])

    @synmedplus.common.decorators.watchdog(WATCHDOG_TRAY_ALARM_EMERGENCY, WD_PromptUserStartProduction)
    def ST_PromptUserStartProduction(self, ctx):
        """
        This state is used to wait for user start production.
        """
        if self.received_yes():
            self.change_state('ST_StartProduction', 'User confirmed, starting production')
            return

        if self.received_no():
            ctx.remove_tray_reason_id = synmedplus.common.constants.PU_PRE_VALIDATION_REFUSED
            self.change_state('ST_ChangeTray', 'User decided not to start production')
            return

    def ST_StartProduction(self, ctx):
        """
        This state is the entry point for production.
        We set the po_file status to PRODUCING and assign the production user id.
        Notify the UI that production started 
        and transition to the main production loop
        """

        # Set PO file status to Producing
        ctx.set_po_file_status(ctx.po_file_id, synmedplus.common.constants.SYNSOFT_PO_FILE_STATUS_IN_PRODUCTION)
        ctx.set_production_user_id(ctx.po_file_id, ctx.user_id)

        self.broadcast(broadcast.PickingUnitProductionStarted(ctx.po_file_id,
                                                              ctx.po_file_name,
                                                              ctx.tray_id,
                                                              ctx.tray_code))

        self.change_state('ST_GetPendingPrescription')

    def WD_GetPendingPrescription(self, ctx, watchdog_result):
        if watchdog_result[0] != synmedplus.common.constants.GALIL_TRAY_INSERTED:
            # This means the tray got removed
            self.restart('Watchdog found out that the tray was removed, restarting')

        elif watchdog_result[1] != synmedplus.common.constants.GALIL_NO_ALARM:
            # This means that the controller has alarms
            ctx.al_next_state = 'ST_GetPendingPrescription'
            self.change_state('ST_QueryAlarmVector', 'Watchdog found out that the controller is in alarm')

        self._validate_buffer_check(watchdog_result[2])

    @synmedplus.common.decorators.watchdog(WATCHDOG_TRAY_ALARM_EMERGENCY, WD_GetPendingPrescription)
    def ST_GetPendingPrescription(self, ctx):
        """
        This is the piece de resistance of the pickingunit FSM.
        It queries the database for the next command to execute and analyses the result
        before switching to the appropriate state.
        
        Many return values have been pre-validated before starting the production so the chance
        of getting them is very slim.
        """

        if not ctx.throttle_no_current_action.is_ready():
            return

        ctx.al_pickupdeposit = False
        ctx.next_operation = ctx.get_next_operation()
        ctx.get_next_status = ctx.next_operation.return_status
        self.log_to_current_state('Get_next_operation returned %s' % ctx.get_next_status)
        ctx.current_operation = synmedplus.services.production.operationstatus.OperationStatus(ctx.next_operation,
                                                                                               ctx.user_id)

        if ctx.current_operation.operation_done.drug_id not in [None, 0, -1]:
            drug_info = ctx.get_drug_id_data(ctx.current_operation.operation_done.drug_id)
            operation_done_in_file = ctx.get_completed_operation_count(ctx.po_file_id)
            if drug_info:

                self.broadcast(broadcast.PickingUnitProductionUpdate(
                    ctx.po_file_id,
                    ctx.po_file_name,
                    ctx.next_operation.tray_id,
                    ctx.next_operation.drug_id,
                    ctx.current_operation.operation_done.drug_container_id,
                    drug_info['primary_drug_name'],
                    drug_info['din'],
                    drug_info['concentration'],
                    ctx.next_operation.operation_done_in_tray,
                    ctx.next_operation.operation_to_do_in_tray,
                    operation_done_in_file,
                    ctx.next_operation.total_operation_number,
                    ctx.next_operation.tray_code)
                )

                ctx.tray_progress = 100 * float(ctx.next_operation.operation_done_in_tray) / ctx.next_operation.operation_to_do_in_tray

                external_containers = ctx.get_required_external_containers(ctx.next_operation.tray_id)
                self.log_to_current_state(external_containers)
                self.broadcast(broadcast.PickingUnitExternalContainersNeeded(external_containers))

                log_msg = "TrayId: %s %s OperationId: %s  CardId: %s  ContainerId: %s  DrugId: %s [(%s)(%s)(%s)]" % (
                    ctx.next_operation.tray_id,
                    ctx.next_operation.tray_code,
                    ctx.current_operation.operation_done.po_file_operation_detail_id[
                        synmedplus.common.constants.FIRST_VALVE],
                    ctx.current_operation.operation_done.card_id,
                    ctx.current_operation.operation_done.drug_container_id,
                    ctx.current_operation.operation_done.drug_id,
                    drug_info['din'],
                    drug_info['primary_drug_name'],
                    drug_info['concentration']
                )
                log_msg += "  "
                log_msg += "PoFileHdrId: %s Valves: %s %s %s %s %s %s %s %s" % (
                    ctx.current_operation.operation_done.po_file_header_id,
                    synmedplus.common.constants.DB_TRUE if ctx.current_operation.bitmask_todo & 1 else synmedplus.common.constants.DB_FALSE,
                    synmedplus.common.constants.DB_TRUE if ctx.current_operation.bitmask_todo & 2 else synmedplus.common.constants.DB_FALSE,
                    synmedplus.common.constants.DB_TRUE if ctx.current_operation.bitmask_todo & 4 else synmedplus.common.constants.DB_FALSE,
                    synmedplus.common.constants.DB_TRUE if ctx.current_operation.bitmask_todo & 8 else synmedplus.common.constants.DB_FALSE,
                    synmedplus.common.constants.DB_TRUE if ctx.current_operation.bitmask_todo & 16 else synmedplus.common.constants.DB_FALSE,
                    synmedplus.common.constants.DB_TRUE if ctx.current_operation.bitmask_todo & 32 else synmedplus.common.constants.DB_FALSE,
                    synmedplus.common.constants.DB_TRUE if ctx.current_operation.bitmask_todo & 64 else synmedplus.common.constants.DB_FALSE,
                    synmedplus.common.constants.DB_TRUE if ctx.current_operation.bitmask_todo & 128 else synmedplus.common.constants.DB_FALSE,
                )
                self.log_to_current_state(log_msg)
                # log all operation ids
                log_msg = "Extra operationId : %s %s %s %s %s %s %s %s" % (
                    ctx.current_operation.operation_done.po_file_operation_id[
                        synmedplus.common.constants.FIRST_VALVE + 0],
                    ctx.current_operation.operation_done.po_file_operation_id[
                        synmedplus.common.constants.FIRST_VALVE + 1],
                    ctx.current_operation.operation_done.po_file_operation_id[
                        synmedplus.common.constants.FIRST_VALVE + 2],
                    ctx.current_operation.operation_done.po_file_operation_id[
                        synmedplus.common.constants.FIRST_VALVE + 3],
                    ctx.current_operation.operation_done.po_file_operation_id[
                        synmedplus.common.constants.FIRST_VALVE + 4],
                    ctx.current_operation.operation_done.po_file_operation_id[
                        synmedplus.common.constants.FIRST_VALVE + 5],
                    ctx.current_operation.operation_done.po_file_operation_id[
                        synmedplus.common.constants.FIRST_VALVE + 6],
                    ctx.current_operation.operation_done.po_file_operation_id[
                        synmedplus.common.constants.FIRST_VALVE + 7]
                )
                self.log_to_current_state(log_msg)
                del log_msg
                # switch on get next result

        if ctx.current_operation.operation_done.drug_id == -1:
            self.log_to_current_state('Obtained operation with drug_id -1, closing it')
            # This is a marker operation that indicates the tray only had external operations
            # mark it as done and the next call should indicate tray/file completed
            ctx.current_operation.set(synmedplus.common.constants.DB_TRUE)
            ctx.current_operation.log_to_db(ctx)
            return

        # 0, everything is fine
        # -9 This tray still has external drug to be put in w/ SynMedAssist but user wanted to continue    
        elif ctx.get_next_status in [synmedplus.common.constants.PU_GET_NEXT_RETURN_OK,
                                     synmedplus.common.constants.PU_GET_NEXT_RETURN_SMA_INCOMPLETE]:

            # skip this if the only todo is on 8th valve
            if ctx.current_operation.bitmask_todo == (1 << synmedplus.common.constants.LAST_VALVE - 1) and \
                            ctx.current_operation.operation_done.card_id != -404:
                # -----------------------------------------------------------
                # If the only valve to be done is the '8th' virtual valve, 
                # We mark it as 'Not done' and move on to the next command.
                # -----------------------------------------------------------
                # We do this for the following reason:
                # If all commands for the current card are ONLY for the 8th valve,
                # we'd try to pick them one by one and thus potentially bypass the virtual card
                # mechanism which optimizes the pick/placement for MTS31.
                # -----------------------------------------------------------
                # Instead now, this will have the effect of letting GET_NEXT_OPERATION
                # get back to us and regroup all commands for virtual card if needed
                ctx.current_operation.set(synmedplus.common.constants.DB_FALSE)
                ctx.current_operation.log_to_db(ctx, 0)
                self.log_to_current_state("get_next_oeration(0 || -5), Skipping potential virtual command")
                return
            else:
                ctx.start_category_timer(PRODUCTION_STATS_TIME, ctx.po_file_id)
                self.change_state('ST_DoPickupDeposit')
                return

        # This pickingunit has no usable drug container on it's buffer
        elif ctx.get_next_status == synmedplus.common.constants.PU_GET_NEXT_RETURN_NO_AVAILABLE_OPS:
            if not self.bmask & TIMING_REFILL:
                count = 1 if ctx.time_stats['category'] != PRODUCTION_STATS_REFILL else 0
                ctx.start_category_timer(PRODUCTION_STATS_REFILL, ctx.po_file_id, count=count)
                self.add_state_message('ST_GetPendingPrescription',
                                       synmedplus.common.fsm.BitmaskMessage(TIMING_REFILL))


            # This timer limits the broadcast rate according to a parameter
            ctx.throttle_no_current_action.reset()
            # Determine what containers this PU is waiting for
            missing_containers = ctx.get_missing_containers()
            self.log_to_current_state('Needed containers: %s' % missing_containers)

            threshold = synmedplus.common.utils.get_config_value(
                'global', 'missing_container_warning_threshold_percent'
            )

            if ctx.should_broadcast_missing_containers(missing_containers, ctx.tray_progress, threshold):
                self.broadcast(broadcast.PickingUnitIdle(ctx.po_file_name,
                                                         ctx.po_file_id,
                                                         ctx.tray_id,
                                                         ctx.tray_code,
                                                         missing_containers))

            # First time we enter here, we start a timer which will trigger
            # After the fsm hasn't progressed for a long time
            if not ctx.timer_no_ops:
                ctx.timer_no_ops = synmedplus.common.utils.ConfigTimer(config_key='pu_no_ops_timer', init=True)

            # Test the timeout timer
            if ctx.timer_no_ops.is_ready():
                self.change_state('ST_RequestSkipDrugs',
                                  'We have been waiting for a container for a while, requesting user input')
                return

        elif ctx.get_next_status == synmedplus.common.constants.PU_GET_NEXT_RETURN_FILE_FINISHED:
            # -3 all commands completed for this po_file 
            self._msg_queue.put(FSM_EVT_LAST_OPR)
            #   go to change tray
            self.change_state('ST_ChangeTray', 'Get next operation(-3): No more prescription for this po_file')
            return

        elif ctx.get_next_status == synmedplus.common.constants.PU_GET_NEXT_RETURN_TRAY_FINISHED:
            # -19 all commands completed for this tray
            # We will bring tray down and wait for a new tray to be inserted\scanned
            #self.add_interstate_message('ST_ChangeTray')
            self._msg_queue.put(FSM_EVT_LAST_OPR)
            self.change_state('ST_ChangeTray', 'Get next operation(-19): No more prescription for this tray')
            return

        # -4 incomplete operation "I"
        elif ctx.get_next_status == synmedplus.common.constants.PU_GET_NEXT_RETURN_INCOMPLETE_OPS:
            self.log_to_current_state(
                "get_next_operation(-4), incomplete operation, marking as completed and logging to db")
            #   Set operation as completed "Y"
            ctx.current_operation.set(synmedplus.common.constants.DB_TRUE)
            #   log to db
            ctx.current_operation.log_to_db(ctx, ctx.current_operation.bitmask_todo,
                                            "PRODUCTION_CARD_ERROR_INCOMPLETE_OPERATION")
            self.log_to_current_state('Operation incomplete, marked it as complete and moving on')
            return

        # -20 User decided to skip container
        elif ctx.get_next_status == synmedplus.common.constants.PU_GET_NEXT_RETURN_INACTIVE_CONTAINER:
            ctx.current_operation.set(synmedplus.common.constants.DB_TRUE)
            ctx.current_operation.log_to_db(ctx, ctx.current_operation.bitmask_todo,
                                            "PRODUCTION_CARD_ERROR_DRUG_INACTIVE_FLAG")
            return

        # -5 internal drug has become external
        elif ctx.get_next_status == synmedplus.common.constants.PU_GET_NEXT_RETURN_INTERNAL_IS_NOW_EXTERNAL:
            # Marking operation as done, with error
            ctx.current_operation.set(synmedplus.common.constants.DB_TRUE)
            ctx.current_operation.log_to_db(ctx, ctx.current_operation.bitmask_todo,
                                            "PRODUCTION_CARD_ERROR_DRUG_EXTERNAL_FLAG")
            return

        # -6 this tray only has external drugs
        elif ctx.get_next_status == synmedplus.common.constants.PU_GET_NEXT_RETURN_ONLY_EXTERNALS:
            # We will not start production on a tray if no drug is to be produced
            # flag tray has no oops
            self.change_state('ST_ChangeTray', 'New tray detected ( no internal meds to deliver, -6)')
            return

        # Any other error code is not handled, stop production
        # default ( catch all unhandled error -7,-8...)
        else:
            # LogToDb on all valves
            # ctx.current_operation.log_to_db(ctx, synmedplus.common.constants.ALL_VALVES_BITMASK, "PRODUCTION_CARD_ERROR_INVALID_OPERATION")

            self.log_to_current_state('ERROR:OPR_GET_NEXT_OERATION returned(%s)' % (ctx.get_next_status))
            self._stop_with_reason(synmedplus.common.constants.STOP_DATABASE_ERROR)
            self.change_state('ST_Stopping', 'Internal DB error detected: stopping')
            return

    def TrOut_GetPendingPrescription(self, ctx):
        # We reset the non progression timer, since, well, we are changing state ain't that progressing?
        ctx.timer_no_ops = None

    def ST_DoPickupDeposit(self, ctx):
        """
        In this state, a concrete pickup/deposit command is sent to the machine, according to the operation 
        we received from the DB.
        Upon completion, we change state according to the completion code of the command.
        
        Note: Once we reach this state, even if a user wants to stop production, the FSM will keep going until
        it has analysed the result of the command ( if the command was sent and if it can still communicate
        with the robot )
        """

        # ===> ST_QueryValveStatus
        # ===> ST_WaitToRetryAfterTimeout
        # ===> ST_DoPickupDeposit
        # =======================================================================
        # cmd = """com=1;syn_id=222;head_number=%s;ranbf=1;colbf=3;tagg=77;tg=2;gc=2;tgl=0;
        #          format=1;v1=1;v2=0;v3=0;v4=0;v5=0;v6=0;v7=0;v8=0;carte=2;per=2;"""%(ctx.head_number)
        # =======================================================================
        if not isinstance(ctx.active_cmd, PickupDepositCmd):
            ctx.al_pickup_deposit = False
            ctx.current_operation.set(synmedplus.common.constants.DB_INCOMPLETE)
            ctx.current_operation.log_to_db(ctx)
            ctx.active_cmd = PickupDepositCmd(self.ctrl, ctx.head_number, self)
            ctx.active_cmd.send(ctx.current_operation.get_galil_cmd_string())

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            ctx.add_to_operation_history(ctx.current_operation)
            ctx.action_sent = ctx.active_cmd.get_cmd_started()

            if not ctx.action_sent:
                # We did not send the command, mark operation back to N
                ctx.current_operation.set(synmedplus.common.constants.DB_FALSE)
                ctx.current_operation.log_to_db(ctx)

            if ctx.active_cmd.status():
                # No error
                self.change_state('ST_QueryValveStatus',
                                  'Pickup deposit command executed successfully, querying valves')

            # If the command ended because of emergency stop, make sure to revalidate the buffer
            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_EMERGENCY_STOP:
                ctx.validate_usable_containers_on_buffer(ctx.tray_id)
                self.change_state('ST_GetPendingPrescription', 'Returning to main loop after emergency stop')

            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_LOST_TRAY:
                # go back to FSM start
                self.restart('Tray vanished, returning to start')

            # The following case means we have detected an alarm and queried it
            # This can be either before or after sending the pickup command
            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM:
                # Get alarm details which have been queried
                ctx.al = ctx.active_cmd.get_alarm()
                ctx.al_next_state = 'ST_GetPendingPrescription'
                # If we detected the alarm after sending the command
                if ctx.action_sent:

                    # Really sent the command but it was not considered because of antp
                    # mark operation as not done and go handle alarms.
                    if ctx.active_cmd.get_antcp() == synmedplus.common.constants.GALIL_ANTICIPATION_ACTIVE:
                        previous_operation = ctx.get_previous_operation()
                        if previous_operation is not None:
                            self.log_to_current_state('Detected alarms for previous operation, antcp=1')
                            ctx.log_alarms_to_db(previous_operation,
                                                 ctx.al,
                                                 antcp=synmedplus.common.constants.GALIL_ANTICIPATION_ACTIVE)
                            ctx.current_operation.set(synmedplus.common.constants.DB_FALSE)
                            ctx.current_operation.log_to_db(ctx)
                            self.change_state('ST_AlarmDetected', 'Detected alarms with anticipation, handle alarms')
                            ctx.active_cmd = None
                            return
                        else:
                            raise synmedplus.common.exceptions.LuckyError(
                                'How do we not have a previous operation whilst being in anticipation!')

                        # Pickup command has been sent
                        # We must query valve status and assess deposit before fixing alarms
                    ctx.al_pickupdeposit = True
                    ctx.log_alarms_to_db(ctx.current_operation, ctx.al)
                    self.change_state('ST_QueryValveStatus',
                                      'PickupDeposit command detected an alarm which will be handled after we query valve status')
                # We detected the alarm before sending the command, mark as not done and resolve alarms,
                else:
                    if not synmedplus.common.utils.alarms_known(self.ctx.al):
                        self._stop_with_reason(synmedplus.common.constants.STOP_UNKNOWN_ALARM)
                        self.change_state('ST_Stopping', 'Galil returned an unknown alarm, stopping')
                        ctx.active_cmd = None
                        return
                    self.change_state('ST_AlarmDetected', 'Detected alarms before we sent the command, handle alarms')

            # Command ended with timeout status
            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_TIMEOUT:

                # If the current command was sent, log the timeout on both the current and the previous command
                # Otherwise, only log the timeout for the previous command
                if ctx.action_sent:
                    ctx.log_timeout('BOTH')
                else:
                    ctx.log_timeout('PREVIOUS')

                self.change_state('ST_Timeout', 'Command ended by a timeout')

            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_SOCKET_ERROR:
                # If the current command was sent, log the timeout on both the current and the previous command
                # Otherwise, only log the timeout for the previous command
                if ctx.action_sent:
                    ctx.log_timeout('BOTH')
                else:
                    ctx.log_timeout('PREVIOUS')

                self.change_state('ST_SocketException',
                                  'Received socket exception error while communicating with galil')

            ctx.active_cmd = None

    def ST_QueryValveStatus(self, ctx):
        """
        This state is used to obtain valve information after a pickup deposit
        (VsomPg, VsomDp) 
        """

        if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.QueryValveStatusCmd):
            ctx.active_cmd = synmedplus.common.galilactioncmd.QueryValveStatusCmd(self.ctrl, self)
            ctx.active_cmd.send()
        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            # the following cases mean that we are unable to communicate with the robot, hence unable to obtain valve status
            # stop production
            if ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_TIMEOUT:
                ctx.log_timeout('CURRENT')
                self.change_state('ST_Timeout', 'Detected timeout')
            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_SOCKET_ERROR:
                ctx.log_timeout('CURRENT')
                self.change_state('ST_SocketException',
                                  'Received socket exception error while communicating with galil')
            # This means the command completed successfully, we have access to the result
            else:
                ctx.current_operation.process_result(ctx.active_cmd.get_result(), ctx.machine_serial)
                self.change_state('ST_AssessPickupDeposit', 'Received valve status, changing to assessement state')

            ctx.active_cmd = None

    def ST_AssessPickupDeposit(self, ctx):
        """
        In this state, we analyse the result of a pickup deposit command based on
        VsomPg and VsomDp vectors
        """
        # ===> ST_GetPendingPrescription
        # ===> ST_DeactivateContainer
        # ===> ST_AlarmDetected ( Set in DoPickupDeposit )
        msg = "Todo: %s VsomPg: %s VsomDp: %s" % (bin(ctx.current_operation.bitmask_todo),
                                                  bin(ctx.current_operation.bitmask_pickedup),
                                                  bin(ctx.current_operation.bitmask_delivered))
        self.log_to_current_state(msg)

        if ctx.current_operation.bitmask_over_delivered:
            self.log_to_current_state('Overdelivered: %s' % (bin(ctx.current_operation.bitmask_over_delivered)))
            DB_ERROR = "PRODUCTION_CARD_ERROR_TOO_MANY_PICKEDUP_FLAG"
            ctx.current_operation.log_to_db(ctx, ctx.current_operation.bitmask_over_delivered, DB_ERROR)

        if ctx.current_operation.bitmask_bad_drop:
            self.log_to_current_state('Bad Drop: %s' % (bin(ctx.current_operation.bitmask_bad_drop)))
            DB_ERROR = "PRODUCTION_CARD_ERROR_BAD_DROP_FLAG"
            ctx.current_operation.log_to_db(ctx, ctx.current_operation.bitmask_bad_drop, DB_ERROR)

        if ctx.current_operation.bitmask_bad_deposit:
            self.log_to_current_state('Bad Deposit: %s' % (bin(ctx.current_operation.bitmask_bad_deposit)))
            DB_ERROR = "PRODUCTION_CARD_ERROR_BAD_DEPOSIT_FLAG"
            ctx.current_operation.log_to_db(ctx, ctx.current_operation.bitmask_bad_deposit, DB_ERROR)

        if ctx.current_operation.is_wrong_container:
            self.log_to_current_state('Pickup drug from wrong container, expected {exp} got {got}'.format(
                exp=ctx.current_operation.next_operation.drug_container_id,
                got=ctx.current_operation.operation_done.container_id_lu))
            # Container number reading is active and we didnt read the expected container number
            # - Flag delivered valves in error
            DB_ERROR = "PRODUCTION_CARD_ERROR_RFID_BAD_CONTAINER"
            ctx.current_operation.log_to_db(ctx, ctx.current_operation.bitmask_delivered, DB_ERROR)
            # - Make sure we wont get more drugs from this container
            ctx.flag_buffer_error(ctx.current_operation.operation_done.working_area_position_x)

        if ctx.current_operation.is_wrong_machine:
            self.log_to_current_state(
                'Attempted to pickup from a container from another machine, expected serial %s, got %s' % (
                    ctx.machine_serial,
                    ctx.current_operation.operation_done.serie_lu))
            DB_ERROR = "PRODUCTION_CARD_ERROR_RFID_BAD_SYNMED"
            ctx.current_operation.log_to_db(ctx, ctx.current_operation.bitmask_delivered, DB_ERROR)

            # On assigne une erreur a l'emplacement en question
            # On marque le contenant comme etant le contenant 0
            ctx.flag_buffer_error(ctx.current_operation.operation_done.working_area_position_x)

        if ctx.current_operation.is_wrong_command_number:
            msg = 'Unexpected command number return from galil, expected {} got {}'.format(
                ctx.current_operation.next_operation.po_file_operation_line_order_id,
                ctx.current_operation.operation_done.po_file_operation_line_order_id)
            self.log_to_current_state(msg)
            # Depending on the global config 'shutdown_on_command_id_mismatch',
            # we either raise an unhandled exception to stop the service or let the error pass.
            if synmedplus.common.utils.get_config_value('global', 'shutdown_on_command_id_mismatch') \
                    == synmedplus.common.constants.CONFIG_TRUE:
                raise synmedplus.common.exceptions.LuckyError(msg)

        if ctx.current_operation.is_considered_complete():
            if ctx.current_operation.bitmask_todo != ctx.current_operation.bitmask_delivered and \
                    not ctx.current_operation.has_multiple_operation_ids:
                ctx.current_operation.set(synmedplus.common.constants.DB_FALSE, ctx.current_operation.bitmask_delivered)
            else:
                ctx.current_operation.set(synmedplus.common.constants.DB_TRUE, ctx.current_operation.bitmask_delivered)

            ctx.current_operation.log_to_db(ctx)

            if ctx.al_pickupdeposit:
                if not synmedplus.common.utils.alarms_known(self.ctx.al):
                    self._stop_with_reason(synmedplus.common.constants.STOP_UNKNOWN_ALARM)
                    self.change_state('ST_Stopping', 'Galil returned an unknown alarm, stopping')
                    return
                self.change_state('ST_AlarmDetected',
                                  'Detected an alarm while processing a pickup deposit cmd, handling it now.')
            elif ctx.user_stop_flag:
                self.change_state('ST_Stopping',
                                  'Received stop signal while processing a picking deposit cmd or its result, stopping now')
            else:
                self.change_state('ST_GetPendingPrescription', 'Everything went well, fetching next command')

        else:
            if ctx.current_operation.bitmask_remaining:
                self.log_to_current_state('Remaining: %s' % (bin(ctx.current_operation.bitmask_remaining)))
                ctx.current_operation.set(synmedplus.common.constants.DB_FALSE, ctx.current_operation.bitmask_delivered)
                ctx.current_operation.log_to_db(ctx)
            else:
                # Not complet but no remaining
                done = ctx.current_operation.bitmask_bad_deposit | ctx.current_operation.bitmask_bad_drop | ctx.current_operation.bitmask_delivered
                self.log_to_current_state("Done (BadDeposit|BitmaskBadDrop|BitmaskDelivered): %s" % (done))
                ctx.current_operation.set(synmedplus.common.constants.DB_TRUE, done)
                ctx.current_operation.log_to_db(ctx)

            # Transition to next state
            if ctx.al_pickupdeposit:
                if not synmedplus.common.utils.alarms_known(self.ctx.al):
                    self._stop_with_reason(synmedplus.common.constants.STOP_UNKNOWN_ALARM)
                    self.change_state('ST_Stopping', 'Galil returned an unknown alarm, stopping')
                    return
                self.change_state('ST_AlarmDetected',
                                  'Detected an alarm while processing a pickup deposit cmd, handling it now.')
            elif ctx.user_stop_flag:
                self.change_state('ST_Stopping',
                                  'Received stop signal while processing a picking deposit cmd or its result, stopping now')
            else:
                if ctx.current_operation.bitmask_remaining:
                    self.change_state('ST_ShakeNeeded', 'Unable to pickup all meds, flag container')
                else:
                    self.change_state('ST_GetPendingPrescription', 'Everything went well, fetching next command')

    def ST_ShakeNeeded(self, ctx):
        """
        The only way to end up in this state is coming from ST_AssessPickupDeposit when a command is:
        - Not considered complete and bitmask.remaining > 0 
        This state is used to notify the DB that we weren't able to fulfill a pickup deposit command completely
        When we get in this state, we call a DB function which increments the shake counter for a specific container.
        Once a container's shake count gets over a parameter value, it is flagged for replenishment.
        Which in turn indicates that this container will be picked up by the transporter into an out cell.
        """
        shake_count = ctx.set_shake_needed(ctx.current_operation.operation_done.drug_container_id)
        if shake_count == 0:
            # Flagged for replenishement
            self.log_to_current_state(
                'Container %s flagged for replenishement' % (ctx.current_operation.operation_done.drug_container_id))
        elif shake_count >= 1:
            # Indicates how many times the container has been shaked
            self.log_to_current_state('Container %s flagged for shake, shake count: %s' % (
                ctx.current_operation.operation_done.drug_container_id, shake_count))
        else:
            # set_shake_needed error
            self.log_to_current_state('Unable to flag container %s for shake, error: %s' % (
                ctx.current_operation.operation_done.drug_container_id, shake_count))

        self.change_state('ST_GetPendingPrescription', 'Mark container to be shaked, getting next operation')

    def TrIn_ChangeTray(self, ctx):
        self.log_to_current_state('Bringing tray down soon.')
        # Set tray status to SYNSOFT_PO_FILE_STATUS_IN_PRODUCTION
        # ctx.set_tray_status(SYNSOFT_PO_FILE_STATUS_IN_PRODUCTION)
        # Hook on eMar/One_Mar/DeliveryTrack for current tray
        ctx.start_category_timer(PRODUCTION_STATS_IDLE)

    def ST_ChangeTray(self, ctx):
        """
        This state is used to bring the tray down after a tray is completed or
        after we detected an error from get_next_oeration or
        user decides to stop production at some point
        
        """
        # ===> ST_ValidatePrevTray

        if not isinstance(ctx.active_cmd, synmedplus.services.production.tray_down.TrayDownCmd):
            ctx.active_cmd = synmedplus.services.production.tray_down.TrayDownCmd(self.ctrl, parentfsm=self)
            ctx.active_cmd.send()

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            # We have an alarm for a previous command and ancitipation is on.
            if ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM and\
                            ctx.active_cmd.get_antcp() == synmedplus.common.constants.GALIL_ANTICIPATION_ACTIVE:
                previous_operation = ctx.get_previous_operation()
                if previous_operation is not None:
                    ctx.log_alarms_to_db(previous_operation, ctx.active_cmd.get_alarm())

            if ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM and\
                            self.bmask & SILENT:
                # WE wanted to make sure the tray was unlocked but encountered an alarm,
                # we'll try to fix it and get back to unlocking the tray
                ctx.al = ctx.active_cmd.get_alarm()
                ctx.al_next_state = 'ST_TestHomingState'
                self.change_state('ST_AlarmDetected', 'Detected alarms in start loop while trying to unlock tray.')
                ctx.active_cmd = None
                return

            if ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_TIMEOUT and\
                            self.bmask & SILENT:
                self.change_state('ST_Timeout', 'Reached timeout or lost connection while attempting to unlock tray')
                return

            if not ctx.active_cmd.status():
                # Failed to bring tray_down,
                if self.bmask & FSM_EVT_LAST_OPR and not ctx.user_stop_flag:
                    # Failed to bring down tray but we are done with this file so its fine.
                    pass
                else:
                    # we still had prescriptions to produce, marking as partial and stopping
                    self.change_state('ST_Stopping',
                                      'Failed to bring tray down and we still had prescription to deliver')
                    return
            ctx.active_cmd = None
        else:
            return

        # Reraise the last opr flag, we might need it in the following state
        if self.bmask & FSM_EVT_LAST_OPR:
            self._msg_queue.put(FSM_EVT_LAST_OPR)

        # Tray brought down
        # Unbind the tray from this pickingunit's working area
        ctx.reset_actual_tray_id()

        if self.bmask & SILENT:
            self.change_state('ST_ChooseTrayInputMethod', 'Made sure we sent com=7 to unlock tray')
        else:
            self.change_state('ST_ValidatePrevTray', 'Bringing down tray after a stop request or tray completed')

    def TrIn_RequestSkipDrugs(self, ctx):
        # AD 2018-01-15: This reset is required since discovering that we sometimes receive duplicate messages
        # from UI in a rapid burst. The cause for this bug is unknown at the moment.
        # This duplicate sending leads to multiple skips messages being generated.
        # Upon receiving the first dynamic message, this state function begins processing.
        # In the meantime, other messages are received and are added to this state function's message queue.
        # These messages linger until we return to this state again where they are read and we proceed to skip
        # the same drugs once again. However, these drugs might not even be part of the tray we are producing or
        # there might not be operations left for theses drugs in the current tray. Therefore, the drug skip database
        # functions catches this discrepancy and returns an error. Since this error is not handled, the service
        # abruptly stops on an unhandled exception.

        # By always clearing messages before expecting new ones, we will never have lingering messages.
        self.reset_state_messages('ST_RequestSkipDrugs')
        self.broadcast(broadcast.PickingUnitRequestSkipDrugs(ctx.get_missing_containers()))

    def WD_RequestSkipDrugs(self, ctx, watchdog_result):
        if watchdog_result[0] != synmedplus.common.constants.GALIL_TRAY_INSERTED:
            # User removed tray
            self.restart('Watchdog detected that the tray has been removed, going back to loop start.')
        elif watchdog_result[1] != synmedplus.common.constants.GALIL_NO_ALARM:
            # Ala is raised
            ctx.al_next_state = 'ST_RequestSkipDrugs'
            self.change_state('ST_QueryAlarmVector', 'Watchdog detected alarms')
        # Test if the head received a container while we have been waiting for user input
        elif ctx.get_next_operation_status() != synmedplus.common.constants.PU_GET_NEXT_RETURN_NO_AVAILABLE_OPS:
            # Get next is no longer returning -13, meaning there is something to do!
            self.change_state('ST_GetPendingPrescription', 'Operation available for this head, fetching next command')

        self._validate_buffer_check(watchdog_result[2])

    @synmedplus.common.decorators.watchdog(WATCHDOG_TRAY_ALARM_EMERGENCY, WD_RequestSkipDrugs)
    def ST_RequestSkipDrugs(self, ctx):
        """
        This state is used to obtain user's input on whether to skip one or many drugs from producing.
        """
        drug_skip_message = self._get_dynamic_message('skip_drugs')

        if drug_skip_message is not None:
            drugs_to_skip = drug_skip_message.get_message()['drugs_to_skip']
            if drugs_to_skip:
                for drug_id in drugs_to_skip:
                    self.log_to_current_state('User decided to skip drug: {c}'.format(c=drug_id))
                    ctx.skip_drug(drug_id)
            self.change_state('ST_GetPendingPrescription', 'User answered request, fetching next command')
            return

    def ST_ValidatePrevTray(self, ctx):
        """
        This state is used to notify the UI that a tray (and possibly a file) is completed.
        It transitions to tray waiting state.
        """
        # ===> ST_ValidateNewTray

        if ctx.get_next_status in [synmedplus.common.constants.PU_GET_NEXT_RETURN_FILE_FINISHED,
                                synmedplus.common.constants.PU_GET_NEXT_RETURN_TRAY_FINISHED]:
            ctx.on_tray_finish(ctx.tray_id, ctx.po_file_name, ctx.po_file_id)
            self.broadcast(broadcast.PickingUnitTrayFinished(ctx.po_file_id, ctx.po_file_name, ctx.tray_id))

        if ctx.get_next_status == synmedplus.common.constants.PU_GET_NEXT_RETURN_FILE_FINISHED:
            ctx.on_file_finish(ctx.po_file_name, ctx.po_file_id)
            self.broadcast(
                broadcast.PickingUnitProductionFinished(ctx.po_file_id, ctx.po_file_name, ctx.tray_id)
            )

        ctx.reset_tray_id()
        if self.bmask & FSM_EVT_LAST_OPR and ctx.user_stop_flag:
            self.change_state('ST_Stopping', 'User force stop')
            return

        if ctx.remove_tray_reason_id is None:
            ctx.remove_tray_reason_id = 0
        self.change_state('ST_PromptRemoveTray', 'Tray operations completed, asking user to remove tray')

    def TrIn_PromptRemoveTray(self, ctx):
        self.broadcast(broadcast.PickingUnitRemoveTray(ctx.remove_tray_reason_id))

    def WD_PromptRemoveTray(self, ctx, watchdog_result):
        if watchdog_result[0] != synmedplus.common.constants.GALIL_TRAY_INSERTED:
            # User removed tray, no longer need to track reason for removal
            ctx.remove_tray_reason_id = None
            self.change_state('ST_Start', 'Watchdog detected that the tray has been removed, going back to loop start.')

        elif watchdog_result[1] != synmedplus.common.constants.GALIL_NO_ALARM:
            # Ala is raised
            ctx.al_next_state = 'ST_PromptRemoveTray'
            self.change_state('ST_QueryAlarmVector', 'Watchdog detected alarms')

    @synmedplus.common.decorators.watchdog(WATCHDOG_TRAY_ALARM, WD_PromptRemoveTray)
    def ST_PromptRemoveTray(self, ctx):
        """
        This state is used to wait until the user informs us the tray has been removed.
        """
        # if ctx.timeout_timer.is_ready():
        #    #We have been waiting for a while for user to remove the tray
        #    pass
        pass

    def ST_ValidateNewTray(self, ctx):
        self.change_state('ST_GetPendingPrescription')

    def ST_QueryAlarmVector(self, ctx):

        if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.QueryAlarmVector):
            ctx.active_cmd = synmedplus.common.galilactioncmd.QueryAlarmVector(self.ctrl, self)
            ctx.active_cmd.send()

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            if ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_TIMEOUT:
                self.change_state('ST_Timeout', 'Detected timeout')
            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_SOCKET_ERROR:
                self.change_state('ST_SocketException',
                                  'Received socket exception error while communicating with galil')
            else:
                ctx.al = ctx.active_cmd.get_result()
                self.change_state('ST_AlarmDetected', 'Alarm detected, obtained alarm vector.')
            ctx.active_cmd = None

    def TrIn_ResetProductionVariables(self, ctx):
        ctx.start_category_timer(PRODUCTION_STATS_IDLE)

    def ST_ResetProductionVariables(self, ctx):
        """
        This state is used to stop the production of a tray and prepare for the next one
        We will clean many variables that indicates we are producing and make sure the status
        of a po file is set properly
        """

        if ctx.po_file_id:
            ctx.set_po_file_status(ctx.po_file_id,
                                   synmedplus.common.constants.SYNSOFT_PO_FILE_STATUS_PARTIALLY_PRODUCED)
        ctx.reset_actual_tray_id()
        ctx.reset_tray_id()

        self.change_state('ST_TryBringTrayDown', 'Reset prodution variables, going to next shutdown state.')

    def ST_TryBringTrayDown(self, ctx):

        if not isinstance(ctx.active_cmd, synmedplus.services.production.tray_down.WrecklessTrayDownCmd):
            ctx.active_cmd = synmedplus.services.production.tray_down.WrecklessTrayDownCmd(self.ctrl, self)
            ctx.active_cmd.send()

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            self.change_state(self.STATE_SHUTDOWN_END, 'Sent bring tray down command.')

    def ST_Stopped(self, ctx):
        """
        This state is used to notify the UI that production has stopped
        It transitions to the initial state.
        """
        stop_reason_msg = self._get_dynamic_message('stop_reason')
        if stop_reason_msg:
            message = stop_reason_msg.get_message()
            stop_reason = message['reason_id']
            stop_detail = message['reason_detail']
        else:
            stop_reason = None
            stop_detail = None

        self.broadcast(broadcast.ComponentStopped(stop_reason, stop_detail))
        self.ctrl.destroy()
        self.change_state(self.IDLE_STATE)

    def TrOut_Stopped(self, ctx):
        self.set_context(PickingCtx(self.ctx.head_number))
        self.ctx.log = getattr(self, 'log_to_current_state')
        self._stopping = False

    # def TrIn_Timeout(self, ctx):
    #     self.broadcast(broadcast.ComponentTimedOut())
    #     self.ctx.user_stop_flag = False

    def ST_Timeout(self, ctx):
        # if self.received_yes():
        self.restart('Detected timeout, restarting')

    def TrIn_AlarmDetected(self, ctx):
        """
        Called once every time we transition to ST_AlarmDetected
        """
        # Alarm hook is used to call a specific function for each alarm ( if defined )
        self.alarm_hook(ctx, ctx.al)
        ctx.start_category_timer(PRODUCTION_STATS_ALARM, ctx.po_file_id, count=len(ctx.al))

    def ST_AlarmDetected(self, ctx):
        # We test if the alarms require user input to fix
        if synmedplus.common.utils.alarms_require_user_input(ctx.al):
            # User input required
            self.change_state('ST_RequestAlarmInput', 'Detected alarms that require user input,prompting for action')
        else:
            self.change_state('ST_ResetAlarm',
                              'Detected alarms but user input was not required to fix them, reseting them.')

    def TrIn_RequestAlarmInput(self, ctx):
        self.broadcast(broadcast.AlarmBroadcast(ctx.al))

    def WD_RequestAlarmInput(self, ctx, watchdog_result):
        if watchdog_result[0] != synmedplus.common.constants.GALIL_TRAY_INSERTED:
            # Tray state changed!
            self.change_state('ST_Start', 'Watchdog detected tray is not longer inserted')
        elif watchdog_result[1] == synmedplus.common.constants.GALIL_NO_ALARM:
            # Alarms are gone!
            self.change_state(ctx.al_next_state, 'Watchdog detected no more alarms!')

        self._validate_buffer_check(watchdog_result[2])

    @synmedplus.common.decorators.watchdog(WATCHDOG_TRAY_ALARM_EMERGENCY, WD_RequestAlarmInput)
    def ST_RequestAlarmInput(self, ctx):
        if self.received_yes():
            self.change_state('ST_ResetAlarm')
            return

    def ST_ResetAlarm(self, ctx):

        if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.AlarmResetCmd):
            ctx.active_cmd = synmedplus.common.galilactioncmd.AlarmResetCmd(ctx.al, self.ctrl, self)
            ctx.active_cmd.send()

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            if self.handle_cmd_completed(ctx.active_cmd, ctx.al_next_state):
                self.change_state(ctx.al_next_state, 'Successfully reset alarms')
            else:
                # this means handle_cmd_completed changed to an error state
                pass
            ctx.active_cmd = None

    def ST_SocketException(self, ctx):
        self.restart('Detected socket exception, restarting')

    # ============= Unused states ==============
    def ST_DeactivateContainer(self, ctx):
        # ===> ST_GetPendingPrescription
        self.change_state('ST_GetPendingPrescription')

    def ST_NoActiveContainer(self, ctx):
        # ===> ST_GetPendingPrescription
        # ===> ST_Refill
        # ===> ST_CancelPrescription
        self.change_state('ST_Refill')

    def ST_Refill(self, ctx):
        # ===> ST_NoActiveContainer
        # ===> ST_GetPendingPrescription
        self.change_state('ST_GetPendingPrescription')

    def ST_CancelPrescription(self, ctx):
        # ===> ST_GetPendingPrescription
        self.change_state('ST_GetPendingPrescription')

    def ST_StartParkTrayForRefill(self, ctx):
        # ===> ST_EndParkTrayForRefill
        self.change_state('ST_EndParkTrayForRefill')

    def ST_EndParkTrayForRefill(self, ctx):
        # ===> [FINISH]
        self.change_state('ST_WaitToStart')

    def ST_StartBringTrayDown(self, ctx):
        # ===> ST_EndBringTrayDown
        self.change_state('ST_EndBringTrayDown')

    def ST_EndBringTrayDown(self, ctx):
        # ===> [FINISH]
        self.change_state('ST_WaitToStart')

    def ST_GetContainer(self, ctx):
        """
        This state is used to validate that the container we want to pickup is where it should be
        """
        # =======================================================================
        # actual_container = ctx.get_actual_container_id(ctx.next_operation.working_area_position_x, ctx.next_operation.working_area_position_y)
        # if actual_container < 0 or actual_container != ctx.next_operation.drug_container_id:
        #     #Either the container we are looking for is not where it should be or a different one is in it's place.
        #     pass
        # else:
        #     #The container we are expecting is where it should be, send the pickup command.
        # =======================================================================
        self.change_state('ST_DoPickupDeposit')
