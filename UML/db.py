import json
from syndispill import OperationCtx


class DatabaseApi:
    """
    This class is the used to handle action request from the GUI for the production FSM
    
    GUI message format : 
    {
        component: "database",
        action   : <method to invoke*>, * without the msg_ part
        args     : {
                    dictionnary for arguments of the method to invoke
                    eg : from_id : 0
                }
    }
    IMPORTANT: args dictionnary keys must match the target method's signature,
      minimally, all arguments without a default value must be provided.
    
    At some point, we might want to merge all database calls into a single file
    For now, each service has it's own file
    """

    def __init__(self, dbname='synsoftdb', username=None, pwd=None, host='localhost', port=5432):
        self.db = OperationCtx.getdbconnection(dbname, username, pwd, host, port)

    def msg_get_po_files(self, from_id=0, orderby='po_file_header_id', num_items=0):
        limit = ''
        if num_items:
            limit = "LIMIT %s" % (num_items)
        sql = """SELECT * FROM t_po_file_header
                 WHERE %s >= %s
                 ORDER BY %s
                 %s""" % (orderby, from_id, orderby, limit)
        po_files = self.db.executedictrows(sql)
        return json.dumps(po_files, default=str)  # Make sure UTF8 chars are OK

    def msg_get_store_information(self):
        sql = """SELECT * FROM v_working_area
                 WHERE working_area_position_actual_drug_container_id is not null
                 OR    working_area_position_actual_error_id != 0
                 ORDER BY working_area_number """
        store = self.db.executedictrows(sql)
        return store  # json.dumps(store, default=str) # Make sure UTF8 chars are OK

    def msg_change_store_position(self, drug_container_id, error_id, position_enabled, rfid_enabled,
                                  working_area_number, position_x, position_y):
        sql = """ UPDATE t_working_area
                SET   working_area_position_actual_drug_container_id = %s,
                      working_area_position_actual_error_id          = %s,
                      working_area_position_enabled                  = '%s',
                      working_area_position_rfid_enabled             = '%s'
                WHERE    working_area_number     = %s
                AND      working_area_position_x = %s
                AND      working_area_position_y = %s
                """ % (
        drug_container_id, error_id, position_enabled, rfid_enabled, working_area_number, position_x, position_y)
        self.db.execute(sql)
        return 0
