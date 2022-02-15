# -*- coding: utf8 -*-
from __future__ import unicode_literals

import os
import sys
import logging
import json
import time
import cherrypy

import synmedplus.database.manager
import synmedplus.database.query
import synmedplus.database.queries
import synmedplus.common.websocket
import synmedplus.common.constants
import synmedplus.common.fsm
import synmedplus.common.xmllog
import synmedplus.common.galilcom
import synmedplus.common.utils
import synmedplus.common.galilactioncmd
import synmedplus.common.decorators
import synmedplus.common.websocket_broadcast as broadcast


WATCHDOG_ALARM = 'ala=?'


class TransporterCmd(synmedplus.common.galilactioncmd.GalilActionCmd):
    """
    Used to issue container transport commands, to move them in the system
    """

    def __init__(self, controller, parentfsm):
        super(TransporterCmd, self).__init__(controller, parentfsm)
        
        self.chksum_vars = {'com', 'syn_id', 'src', 'ran', 'col', 'dest', 'rand',
                            'cold', 'tagg', 'gc', 'tg', 'tgl', 'sk', 'lect', 'vrfid'}
        self.cleanup_query = 'rfid_lu=0;serie_lu=0;gc_lu=0;synidf=0;ct_on=0;'


class TransporterOperation():
    def __init__(self, db_dict):
        self.return_status = 0
        
        self.transporter_operation_id = 0
        self.operation_transporter_done = ''
        self.po_file_header_id = 0
        
        self.tray_id = 0
        self.drug_id = 0
        self.drug_container_id = 0
        
        self.container_working_area_number = 0
        self.container_position_x = 0
        self.container_position_y = 0
        
        self.container_type = 0
        
        self.aspiration_pipe_diameter = 0
        self.toggle_enable_flag = ''
        self.shake = 0
        
        self.lect = 0
        
        self.vrfid = 0
        self.galil_command_number = 0
        self.working_area_number_from = 0
        
        self.working_area_position_x_from = 0
        
        self.working_area_position_y_from = 0
        self.working_area_number_to = 0
        self.working_area_position_x_to = 0
        self.working_area_position_y_to = 0
        self.from_db(db_dict)

    def from_db(self, db_dict):
        self.__dict__ = dict(db_dict)

    
    def galil_cmd_string(self):
        cmd = ""
        
        cmd += "com=%s;" % (self.galil_command_number)
        
        cmd += "syn_id=%s;" % (self.transporter_operation_id)
        
        cmd += "tagg=%s;" % (self.drug_container_id)
        cmd += "src=%s;" % (self.working_area_number_from)
        
        cmd += "ran=%s;" % (self.working_area_position_y_from)
        cmd += "col=%s;" % (self.working_area_position_x_from)
        
        cmd += "dest=%s;" % (self.working_area_number_to)
        
        cmd += "rand=%s;" % (self.working_area_position_y_to)
        
        cmd += "cold=%s;" % (self.working_area_position_x_to)
        
        cmd += "gc=%s;" % (self.container_type)
        cmd += "tg=%s;" % (self.aspiration_pipe_diameter)
        flag = "1" if self.toggle_enable_flag == "Y" else "0"
        
        cmd += "tgl=%s;" % (flag)
        
        cmd += "lect=%s;" % (self.lect)
        
        cmd += "vrfid=%s;" % (self.vrfid)
        
        cmd += "sk=%s;" % (self.shake)
        # Make sure the command doesnt end with a semi-colon ;
        cmd = cmd[:-1] if cmd[-1] == ';' else cmd

        return cmd


class TransporterCtx(synmedplus.common.fsm.FsmContext):
    """
    We attempt to decouple an FSM from it's operating data/context
    """

    def __init__(self):
        super(TransporterCtx, self).__init__()
        self.cmd = None
        self.active_cmd = None
        self.action_sent = False

        self.timer_connection = synmedplus.common.utils.ConfigTimer(config_key='connection_retry_throttle',
                                                                    auto=True,
                                                                    init=False)
        self.throttle_no_current_action = synmedplus.common.utils.ConfigTimer(config_key='get_next_throttle_tu',
                                                                              init=False, auto=False)
        self.timer_watchdog = synmedplus.common.utils.ConfigTimer(config_key='watchdog_timer', init=True)

        self.next_operation = None
        self.rfid_lu = None
        self.gc_lu = None

    def on_database_connected(self):
        super(TransporterCtx, self).on_database_connected()

    def get_next_operation(self):
        query = synmedplus.database.query.Query("SELECT * from opr.opr_get_next_operation_transporter();")
        self.execute_query(query)

        # __DB_CHRON__
        
        if synmedplus.common.utils.get_config_value('global',
                                                    'monitor_db_chron') == synmedplus.common.constants.CONFIG_TRUE:
            q = synmedplus.database.queries.get_db_chron_query('OPR_GET_NEXT_OPERATION_TRANSPORTER')
            self.execute_query(q)
            synmedplus.common.utils.log_db_chron_differential(q.get_result(), query.get_query_time())

        return TransporterOperation(query.get_result()[0]) if query.has_result() else None

    def _set_operation_done(self, opr_id, opr_status, rfid_lu, gc_lu, serie_lu):
        query = synmedplus.database.query.Query(
            """SELECT * from opr.opr_set_operation_transporter_done(%(opr_id)s ,
            %(status)s ,
            %(rfid_lu)s ,
            %(gc_lu)s ,
            %(serie_lu)s
            )""",
            {'opr_id': opr_id,
             'status': opr_status,
             'rfid_lu': rfid_lu,
             'gc_lu': gc_lu,
             'serie_lu': serie_lu})

        self.execute_query(query)

        # __DB_CHRON__
        if synmedplus.common.utils.get_config_value('global',
                                                    'monitor_db_chron') == synmedplus.common.constants.CONFIG_TRUE:
            q = synmedplus.database.queries.get_db_chron_query('OPR_SET_OPERATION_TRANSPORTER_DONE')
            self.execute_query(q)
            synmedplus.common.utils.log_db_chron_differential(q.get_result(), query.get_query_time())

        return query.get_result()[0]['opr_set_operation_transporter_done'] if query.has_result() else None

    def create_transport_operation(self, drug_container_id, tray_id,
                                   working_area_from, working_area_from_x, working_area_from_y,
                                   working_area_to, working_area_to_x, working_area_to_y, select_number):
        
        sql_query = """SELECT * from opr.opr_insert_operation_transporter(%(container_id)s,
        %(tray_id)s,
        %(from)s,
        %(from_x)s,
        %(from_y)s,
        %(to)s,
        %(to_x)s,
        %(to_y)s,
        %(select_number)s
        )"""
        query = synmedplus.database.query.Query(sql_query, {'container_id' :drug_container_id,
                                                            'tray_id': tray_id,
                                                            'from': working_area_from,
                                                            'from_x': working_area_from_x,
                                                            'from_y': working_area_from_y,
                                                            'to': working_area_to,
                                                            'to_x': working_area_to_x,
                                                            'to_y': working_area_to_y,
                                                            'select_number': select_number})
        self.execute_query(query)
        return query.get_result()[0]['opr_insert_operation_transporter'] if query.has_result() else None

    def _create_transport_operation_for_error(self):
        # (p_drug_container_id , p_tray_id , (FROM), (TO) )
        # (0, NULL, NULL, NULL ,NULL , 0 ,1 ,1,0)
        
        return self.create_transport_operation(0, None, None, None, None, 0, 1, 1, 0)

    def _set_operation_error(self, opr_id, error_name):
        
        query = synmedplus.database.query.Query(
            "SELECT * from opr.opr_set_operation_transporter_error(%(opr_id)s ,%(error_name)s )",
            query_parameters={'opr_id': opr_id, 'error_name': error_name})
        self.execute_query(query)
        return query.get_result()[0]['opr_set_operation_transporter_error'] if query.has_result() else None

    def get_error_names_from_alarm_vector(self, alvect):
        return [synmedplus.common.constants.SYNSOFT_GENERIC_ALARM_STR + "_%03d" % (alarm) for alarm in alvect]

    def set_operation_done_incomplete(self, opr_id):
        
        return self._set_operation_done(opr_id, synmedplus.common.constants.DB_INCOMPLETE, None, None, None)

    def set_operation_done_complete(self, opr_id, rfid_lu, gc_lu, serie_lu):
        
        return self._set_operation_done(opr_id, synmedplus.common.constants.DB_TRUE, rfid_lu, gc_lu, serie_lu)

    def set_operation_done_error(self, opr_id, rfid_lu=0, gc_lu=0, serie_lu=0, errors=[]):
        for error in errors:
            self._set_operation_error(opr_id, error)

        return self._set_operation_done(opr_id, synmedplus.common.constants.DB_ERROR, rfid_lu, gc_lu, serie_lu)

    def close_cmd_before_sent(self, current_opr_id, alvect, rfid_lu, gc_lu, serie_lu):
        # This command is used to properly close a command and insert a new one for detected errors
        # We detected an alarm before sending a new command (that we need to close)

        # we close the command to be sent with error NEVER_DONE
        self.set_operation_done_error(current_opr_id, errors=[synmedplus.common.constants.TU_NOT_COMPLETED_CMD_ERROR])

        # Obtain a new transporter operation ID to log errors onto
        opr_id = self._create_transport_operation_for_error()
        # Obtain error names from alarm vector

        errors = self.get_error_names_from_alarm_vector(alvect)
        # log all alarms/errors to this new opr_ID and close the operation_id with error
        
        self.set_operation_done_error(opr_id, rfid_lu=0, gc_lu=0, serie_lu=0, errors=errors)

    def create_operation_for_alarms(self, alvect):
        # Obtain a new transporter operation ID to log errors onto
        opr_id = self._create_transport_operation_for_error()
        # Obtain error names from alarm vector

        errors = self.get_error_names_from_alarm_vector(alvect)
        # log all alarms/errors to this new opr_ID and close the operation_id with error
        
        self.set_operation_done_error(opr_id, rfid_lu=0, gc_lu=0, serie_lu=0, errors=errors)

    def reactivate_transporter(self):
        query = synmedplus.database.query.Query("""
                       update public.t_working_area as a
                       set    working_area_position_enabled = 'Y'
                       where  a.working_area_number         = 0
                       and    a.working_area_position_x     = 1
                       and    a.working_area_position_y     = 1""")
        self.execute_query(query)

    def flag_working_area(self, zone, x, y):
        sql_query = synmedplus.database.query.Query("""
                                     update t_working_area
                                     set    working_area_position_actual_error_id = 3,
                                            working_area_position_actual_drug_container_id = 0
                                     where  working_area_number = %(zone)s
                                     and    working_area_position_x = %(x)s
                                     and    working_area_position_y = %(y)s """,
                                                    {'zone': zone, 'x': x, 'y': y})
        self.execute_query(sql_query)


class Transporter(synmedplus.common.fsm.ServiceFsm):
    """
    The Transporter is in charge of moving containers within the SynmedPlus
    """
    STATE_INIT_FIRST = 'ST_InitController'
    STATE_MAIN_FIRST = 'ST_Start'

    def __init__(self, name=synmedplus.common.constants.FSM_TRANSPORTER_NAME, ctx=None, parentfsm=None, logs_dir=None):
        self.unstoppable_states = ['ST_Transport', 'ST_AssessTransportResult']
        if ctx is None:
            ctx = TransporterCtx()
        if not logs_dir:
            thisdir = os.path.abspath(os.path.dirname(__file__))
            logs_dir = os.path.join(thisdir, synmedplus.common.constants.SERVICE_LOG_DIR)
        super(Transporter, self).__init__(name, 
                                          ctx, 
                                          parentfsm=parentfsm, 
                                          logs_folder=logs_dir,
                                          create_query_manager=True)
        
        cherrypy.engine.subscribe(self.get_name(), self._handle_websocket_message)
        self.ctrl = None
        self._init_controller()
        
        self.alarm_hook_dict = {
            89: ['disable_working_area_hook', 'flag_bad_rfid_tag_hook'],
            90: ['disable_working_area_hook', 'flag_bad_rfid_tag_hook'],
            91: ['flag_bad_rfid_tag_hook'],
            92: ['flag_bad_rfid_tag_hook'],
            160: ['disable_working_area_hook', 'flag_bad_rfid_tag_hook']
        }

    def get_loop_sleep(self):
        return self._config.loop_sleep

    def alarm_hook(self, ctx, alarm_list):

        alarm_set = set(alarm_list)
        alarm_set.discard(0)

        for alarm in alarm_set:
            
            if self.alarm_hook_dict.has_key(alarm):
                # alarms can have 1 or more hooks associated with them
                hooks = self.alarm_hook_dict[alarm]
                # Funnel function calls
                if isinstance(hooks, str):
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

    def flag_bad_rfid_tag_hook(self, ctx, alarm_num):
        if ctx.next_operation and ctx.next_operation.drug_container_id:
            self.log_to_current_state(
                'BAD_RFID_TAG: {al},{c}'.format(al=alarm_num, c=ctx.next_operation.drug_container_id))

    def disable_working_area_hook(self, ctx, alarm_num):
        ctx.flag_working_area(ctx.next_operation.working_area_number_from,
                              ctx.next_operation.working_area_position_x_from,
                              ctx.next_operation.working_area_position_y_from)

    def _init_controller(self):
        if not self.ctrl:
            self.ctrl = synmedplus.common.galilcom.Controller(self.get_name(), self, self._config.host, self._config.port,
                                                              self._config.com_sleep)
            self.ctrl.set_logger_name(self._log_name)

    def destroy_controller(self):
        if not self.ctrl:
            return
        self.ctrl.destroy()
        self.ctrl = None

    def handle_cmd_completed(self, cmd):
        """
        This method was introduced to prevent code duplication when testing
        for a command completion code. It centralises state changes for common
        error cases such as timeouts, socket errors and alarms which are present
        in almost all FSM command sent to galil.
        
        """
        success = False

        self.ctx.action_sent = cmd.get_cmd_started()
        if cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM:
            self.ctx.al = cmd.get_alarm()  # Get alarm details which have been queried
            self.change_state('ST_AlarmDetected')
        elif cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_TIMEOUT:
            self.change_state('ST_Timeout')
        elif cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_SOCKET_ERROR:
            self.change_state('ST_SocketException', 'Received socket exception error while communicating with galil')
        elif cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM_NONE:
            self.ctx.al = None
            self.log_to_current_state('Attempted to reset alarms but they were already fixed')
            success = True
        elif cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_ALARM_NEW:
            self.ctx.al = cmd.get_alarm()
            self.change_state('ST_AlarmDetected',
                              'Attempted to reset alarms but alarm vector changed while processing the request, new alvect : %s' % self.ctx.al)
        else:
            success = True

        return success

    def _pre_update(self):
        """
        Called from the same thread as update(), OK to affect FSM here
        """
        
        if self.should_stop():
            if not self._critical_stop and self.get_statename() in self.unstoppable_states:
                self._msg_queue.put(synmedplus.common.fsm.FSM_STOP)
                return
            # Going to stop,
            self.ctx.active_cmd = None
            self.change_state('ST_Stopping', 'STOP_TRANSPORTER')

    def ST_InitController(self, ctx):
        self._init_controller()
        self.change_state('ST_EstablishDatabaseConnection', 'Received start, testing connection')

    def ST_EstablishDatabaseConnection(self, ctx):

        if ctx.timer_connection.is_ready():
            self.dbm.connect()
            if self.dbm.connected():
                ctx.on_database_connected()
                ctx.timer_connection.init()
                self.broadcast(broadcast.DatabaseConnectionEstablished())
                
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
                message = 'Newer versions are not allowed, stopping'
                self.change_state('ST_Stopping', message)
                raise Exception(message)

    def TrIn_TestRobotConnection(self, ctx):
        self.broadcast(broadcast.RobotConnectionAttempt())

    def ST_TestRobotConnection(self, ctx):
        """
        This state is used to test if the FSM can connect to the robot.
        If it can't, It will indefinately loop in this state and periodically retry to connect
        The reconnect delay is based on a global config parameter
        Once a connection is made, we move on to the next step.
        """

        if ctx.timer_connection.is_ready():
            test_result = synmedplus.common.utils.test_socket_open(self._config.host, self._config.port)
            if test_result == synmedplus.common.constants.OK:
                ctx.timer_connection.init()
                self.change_state('ST_TestGalilVersion', 'Successfully connected to robot')
                self.broadcast(broadcast.RobotConnectionEstablished())
                return
            else:
                
                msg = 'Unable to connect to %s (%s, %s) is closed, error: %s. Retrying in %d seconds' % (
                    self.get_name(), self._config.host, self._config.port, test_result, ctx.timer_connection.get_delay())
                self.log_to_current_state(msg)
                self.broadcast(broadcast.RobotConnectionFailed(ctx.timer_connection.get_delay()))

    def ST_TestGalilVersion(self, ctx):
        if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.QueryGalilVersion):
            ctx.active_cmd = synmedplus.common.galilactioncmd.QueryGalilVersion(self.ctrl, self)
            ctx.active_cmd.send()

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            if self.handle_cmd_completed(ctx.active_cmd):
                ctx.galil_version = ctx.active_cmd.get_result()
                self.log_to_current_state('Galil Version: {version}'.format(version=ctx.galil_version['raw']))
                minimum_accepted_version = synmedplus.common.utils.get_config_value(
                    'version', 'minimum_transporter_version')
                if ctx.galil_version['raw'] < minimum_accepted_version :
                    # The galil version we queried is lower than the one we expect
                    # stopping with warning
                    self._stop_with_reason(synmedplus.common.constants.STOP_GALIL_VERSION)
                    self.change_state('ST_Stopping',
                                      'Transporter version ({}) is lower than minimum ({})'.format(
                                          ctx.galil_version['raw'],
                                          minimum_accepted_version)
                                      )
                else:
                    self.change_state('ST_TestHomingState', 'Galil Version is fine')
            ctx.active_cmd = None

    def TrIn_TestHomingState(self, ctx):
        self.broadcast(broadcast.WaitingForHoming())

    def ST_TestHomingState(self, ctx):

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
                if hstate == synmedplus.common.constants.GALIL_HOMED_STATE:
                    self.change_state(self.STATE_INIT_END, 'Robot is homed.')

                if ala and not hstate == synmedplus.common.constants.GALIL_HOMED_STATE:
                    self._stop_with_reason(
                        synmedplus.common.constants.STOP_ALARM_PREVENTS_HOMING,
                        {'alarms': list(alarms)}
                    )
                    self.change_state('ST_Stopping', 'Alarm prevents homing from completing, stopping')

            ctx.active_cmd = None

    def ST_Start(self, ctx):
        self.broadcast(broadcast.ComponentStarted())
        self.change_state('ST_GetPendingOperation')

    def WD_GetPendingOperation(self, ctx, watchdog_result):
        if watchdog_result[0] != 0:
            # This means that we have alarms for the transporter
            ctx.al_next_state = 'ST_GetPendingOperation'
            self.change_state('ST_QueryAlarmVector', 'Watchdog found out that the controller is in alarm')

    @synmedplus.common.decorators.watchdog(WATCHDOG_ALARM, WD_GetPendingOperation)
    def ST_GetPendingOperation(self, ctx):
        """
        This state is used to obtain the next transport operation.
        It also handles common negative values from get_next_tranporter
        """

        if not ctx.throttle_no_current_action.is_ready():
            return

        # Request the next transport operation from the database
        ctx.next_operation = ctx.get_next_operation()

        # Switch on return status
        
        self.log_to_current_state('get_next_operation_transporter returned (%s)' % (ctx.next_operation.return_status))

        
        if ctx.next_operation.return_status == synmedplus.common.constants.TU_GET_NEXT_RETURN_OK:
            
            self.change_state('ST_Transport', 'Received a valid transport operation')
            return

        elif ctx.next_operation.return_status in [synmedplus.common.constants.TU_GET_NEXT_RETURN_NOTHING_TO_DO,
                                                  synmedplus.common.constants.TU_GET_NEXT_RETURN_BAD_INSERT_OK,
                                                  synmedplus.common.constants.TU_GET_NEXT_RETURN_BAD_TRAY]:
            # Transporter has nothing to do
            # Throttle calls to get_next to prevent from flooding controllers
            ctx.throttle_no_current_action.reset()
            self.broadcast(broadcast.ComponentIdle())

        elif ctx.next_operation.return_status == synmedplus.common.constants.TU_GET_NEXT_RETURN_INCOMPLETE_CMD:
            # Got an operation with status 'I'
            # Marker lerreur TU_INCOMPLETE_CMD_ERROR
            
            self.log_to_current_state(
                'Received operation with status I (id: %s), closing w/error' % ctx.next_operation.transporter_operation_id)
            
            ctx.set_operation_done_error(ctx.next_operation.transporter_operation_id,
                                         rfid_lu=ctx.next_operation.drug_container_id,
                                         gc_lu=ctx.next_operation.container_type,
                                         errors=[synmedplus.common.constants.TU_INCOMPLETE_CMD_ERROR])

        elif ctx.next_operation.return_status == synmedplus.common.constants.TU_GET_NEXT_RETURN_NEVER_DONE_CMD:
            # Got an operation with status 'N'
            # Mark w/ PRODUCTION_CARD_ERROR_OPERATION_NEVER_DONE
            
            self.log_to_current_state(
                'Received operation with status N (id: %s), closing w/error' % ctx.next_operation.transporter_operation_id)
            
            ctx.set_operation_done_error(ctx.next_operation.transporter_operation_id,
                                         rfid_lu=ctx.next_operation.drug_container_id,
                                         gc_lu=ctx.next_operation.container_type,
                                         errors=[synmedplus.common.constants.TU_NOT_COMPLETED_CMD_ERROR])

        elif ctx.next_operation.return_status == synmedplus.common.constants.TU_GET_NEXT_RETURN_IS_INACTIVE:
            # Transporter's working area has been disabled on purpose
            # We will warn the User and wait for him to trigger homing
            
            self.change_state('ST_TransporterResetPrompt',
                              'Warning user about incoming homing, waiting for confirmation')

        else:
            # Dans tous les autres cas, il s'agit d'une erreur grave et on doit arreter
            # Le gestion de requete DB attrapera tous les autres cas qui ne sont pas gerer ci-dessus
            # Nous ne pouvons pas nous rendre ici, a moins d'avoir mal defini le fichier json des valeur de retour
            raise Exception('QueryManager error or db_return_values.json file handled property is wrong')

    def TrIn_Transport(self, ctx):

        self.broadcast(broadcast.TransporterMoveContainer(
            drug_container_id=ctx.next_operation.drug_container_id,
            source_working_area=ctx.next_operation.working_area_number_from,
            destination_working_area=ctx.next_operation.working_area_number_to)
        )

    def ST_Transport(self, ctx):
        """
        This state is used to send a transport command to galil wand wait for its completion
        """

        if ctx.active_cmd is None:
            ctx.active_cmd = TransporterCmd(self.ctrl, self)
        ctx.active_cmd.send(ctx.next_operation.galil_cmd_string())
        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():

            
            if ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_TIMEOUT:
                self.change_state('ST_Timeout')
            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_SOCKET_ERROR:
                
                self.change_state('ST_SocketException',
                                  'Received socket exception error while communicating with galil')
            else:
                # Did we send the command?
                ctx.action_sent = ctx.active_cmd.get_cmd_started()
                # Were there alarms?
                
                ctx.al = ctx.active_cmd.get_alarm()
                # What rfid tag did we read?
                
                ctx.rfid_lu = ctx.active_cmd.get_rfid_lu()
                # What serial tag did we read?
                ctx.serie_lu = ctx.active_cmd.get_serie_lu()
                # What container size did we detect?
                
                ctx.gc_lu = ctx.active_cmd.get_gc_lu()
                # Did we carry a container?
                
                ctx.ct_on = ctx.active_cmd.get_ct_on()
                self.change_state('ST_AssessTransportResult', 'Transport command completed, testing result')

            ctx.active_cmd = None

    def ST_AssessTransportResult(self, ctx):
        """
        This state is used to interpret the result of a transport command.
        There are three cases to test
        1- The command has been sent but alarms were triggered
        2- The command has not been sent because alarms were present
        3- The command has been sent and no alarm were triggered, everything is fine
        """

        if ctx.al:
            
            if ctx.action_sent:
                # Case 1
                self.log_to_current_state('Transport command ended with alarms')
                errors = ctx.get_error_names_from_alarm_vector(ctx.al)
                
                ctx.set_operation_done_error(ctx.next_operation.transporter_operation_id, rfid_lu=ctx.rfid_lu,
                                             gc_lu=ctx.gc_lu, serie_lu=ctx.serie_lu, errors=errors)


            else:
                # Case 2
                self.log_to_current_state('Detected alarms before galil received the transport order')
                
                ctx.close_cmd_before_sent(ctx.next_operation.transporter_operation_id, ctx.al, ctx.rfid_lu, ctx.gc_lu,
                                          ctx.serie_lu)

            # In both cases we must handle alarms after closing operations
            self.change_state('ST_AlarmDetected', 'Detected alarms while processing command')

        else:
            # If we were moving something to an OUT cell, must beep

            # if ctx.next_operation.working_area_number_to == 4:
            #    synmedplus.common.utils.send_beep(self.ctrl, self, beep_count=4)

            # Case 3
            self.log_to_current_state('Transport command ended successfully')
            
            ctx.set_operation_done_complete(ctx.next_operation.transporter_operation_id, ctx.rfid_lu, ctx.gc_lu,
                                            ctx.serie_lu)
            self.change_state('ST_GetPendingOperation', 'Closed operation successfully, requesting next one')

    def TrIn_AlarmDetected(self, ctx):
        self.alarm_hook(ctx, ctx.al)

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
        if watchdog_result[0] == 0:
            # Alarms are gone!
            self.change_state('ST_Start', 'Watchdog detected no more alarms!')

    @synmedplus.common.decorators.watchdog(WATCHDOG_ALARM, WD_RequestAlarmInput)
    def ST_RequestAlarmInput(self, ctx):
        """
        This state is used to wait until the user wants to reset the alarm
        """

        if self.received_yes():
            self.change_state('ST_ResetAlarm')
            return

    def TrIn_TransporterResetPrompt(self, ctx):
        # Commented user request until we find a good reason to reactivate it
        # self.broadcast(broadcast.TransporterRequestHoming())
        pass

    def WD_TransporterResetPrompt(self, ctx, watchdog_result):
        
        if watchdog_result[0] > 0:
            # This means that we have alarms for the transporter
            ctx.al_next_state = 'ST_TransporterResetPrompt'
            
            self.change_state('ST_QueryAlarmVector', 'Watchdog found alarms')

    # @synmedplus.common.decorators.watchdog(WATCHDOG_ALARM, WD_TransporterResetPrompt)

    def ST_TransporterResetPrompt(self, ctx):
        """
        This state is used to wait until the user wants to send the homing command
        Or reactivate the transporter
        """
        # if self.received_yes():
        #    ctx.reactivate_transporter()
        #    self.change_state('ST_Start', 'User gave the go to reset the transporter')

        # Removed user request, simply reactivate transporter and restart
        ctx.reactivate_transporter()
        self.change_state('ST_Start', 'Reactivated transporter')

    def ST_ResetAlarm(self, ctx):
        """
        This state is used to send the alarm reset command and wait for its completion
        """
        if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.AlarmResetCmd):
            ctx.active_cmd = synmedplus.common.galilactioncmd.AlarmResetCmd(ctx.al, self.ctrl, self)
            ctx.active_cmd.send()
        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            if self.handle_cmd_completed(ctx.active_cmd):

                self.change_state('ST_GetPendingOperation',
                                  'Alarms were successfully reset, fetching next transport cmd')
            else:
                # This means handle_cmd_completed already changed to an error state, do nothing
                pass
            ctx.active_cmd = None

    def ST_QueryAlarmVector(self, ctx):

        if not isinstance(ctx.active_cmd, synmedplus.common.galilactioncmd.QueryAlarmVector):
            ctx.active_cmd = synmedplus.common.galilactioncmd.QueryAlarmVector(self.ctrl, self)
            ctx.active_cmd.send()

        ctx.active_cmd.update()

        if ctx.active_cmd.has_completed():
            if ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_TIMEOUT:
                
                self.change_state('ST_Timeout',
                                  'Received timeout stop while querying valve status, stopping production')
            elif ctx.active_cmd.get_error() & synmedplus.common.galilactioncmd.CompletionCode.STATUS_CMD_SOCKET_ERROR:
                self.change_state('ST_SocketException',
                                  'Received socket exception error while communicating with galil')
            else:
                ctx.al = ctx.active_cmd.get_result()
                ctx.create_operation_for_alarms(ctx.al)
                self.change_state('ST_AlarmDetected', 'Alarm detected, obtained alarm vector.')
            ctx.active_cmd = None
            return

    def ST_Timeout(self, ctx):
        # self.broadcast(broadcast.ComponentTimedOut())
        self.restart('Detected timeout, restarting.')

    def ST_SocketException(self, ctx):
        self.restart('Detected socket exception, restarting.')

    def ST_Stopped(self, ctx):
        self.ctrl.destroy()
        ctx.active_cmd = None
        self.change_state(self.IDLE_STATE)

    def TrOut_Stopped(self, ctx):
        # check if we got a message specifying why we stopped.

        stop_reason_msg = self._get_dynamic_message('stop_reason')
        if stop_reason_msg:
            message = stop_reason_msg.get_message()
            stop_reason = message['reason_id']
            stop_detail = message['reason_detail']
        else:
            stop_reason = synmedplus.common.constants.STOP_USER_REQUESTED
            stop_detail = None

        self.broadcast(broadcast.ComponentStopped(stop_reason, stop_detail))
        self.set_context(TransporterCtx())
        self._stopping = False
