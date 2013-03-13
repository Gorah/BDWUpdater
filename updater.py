import csv
import pyodbc
import sys
import datetime
from contextlib import contextmanager

MIG_IDS = {}
TERMINATION_LIST = []
ROWS_TO_PROCESS = []


@contextmanager
def get_connection():
    """
    Prepare connection to database
    """
    cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=localhost;DATABASE=BPO_DataWarehouse;UID=Admin;PWD=30aXH15lx92')
    yield cnxn.cursor()
    cnxn.commit()


def openfile_and_preload():
    """
    Function is opening the file and appends each employee ID to the
    list for later checking
    """
    f = open('uploadData.csv', 'r')
    csvdata = csv.reader(f)
    for row in csvdata:
        MIG_IDS[row[1]] = True
        ROWS_TO_PROCESS.append(row)
        
        
def check_terminations():
    """
    This function is checking all of the ID from tHR_Employee table
    and looking for cases where there's no match against tHR_InMIG.
    This means that employee has been terminated and needs to be
    removed from BDW.
    """
    with get_connection() as cursor:
        cursor.execute("""SELECT ID
                          FROM vw_HR_ActiveEEList""")
        for row in cursor.fetchall():
            if not MIG_IDS.get(row[0]):
                TERMINATION_LIST.append(row[0])

                
def terminate_employees(stamp_date):
    """
    Dispatches employees for termination using list of employees and
    sending it to task functions
    """
    with get_connection() as cursor:
        for eeid in TERMINATION_LIST:
            row_id = find_actions_record(eeid, cursor)
            close_actions_record(row_id, stamp_date, cursor)
            insert_new_action(eeid, stamp_date, cursor)
            row_id = find_job_details_record(eeid, cursor)
            close_job_details(eeid, stamp_date, cursor)
            row_id = find_team_record(eeid, cursor)
            close_team_record(eeid, stamp_date, cursor)
            remove_opsmru(eeid, cursor)           
            
            
def find_actions_record(eeid, cursor):
     """
     This function finds most recent Action record for employee
     """
     cursor.execute("""SELECT TOP(1) ID
                       FROM tHR_Actions
                       WHERE EEID = ?
                       ORDER BY EndDate DESC, ID DESC""", eeid)
     row = cursor.fetchone()
     return int(row[0])

            
def find_job_details_record(eeid, cursor):
    """
    Function to find record id of the most recent entry in
    tHR_JobDetails for terminated employee
    """
    cursor.execute("""SELECT TOP(1) ID
                      FROM tHR_JobDetails
                      WHERE EEID = ?
                      ORDER BY EndDate DESC, ID DESC""", eeid)
    row = cursor.fetchone()
    return int(row[0])


def find_team_record(eeid, cursor):
     """
     This function finds most recent team assigment record for employee
     """
     cursor.execute("""SELECT TOP(1) ID
                       FROM tHR_TeamMembers
                       WHERE EEID = ?
                       ORDER BY EndDate DESC, ID DESC""", eeid)
     row = cursor.fetchone()
     return int(row[0])

     
def close_actions_record(row_id, stamp_date, cursor):
    """
    This function is closing existing action record for terminated
    employee in tHR_Actions table
    """
    enddate = datetime.date(int(stamp_date[:4]),
                            int(stamp_date[5:7]), 01)
    enddate = enddate - datetime.timedelta(days=1)
    enddate = enddate.strftime("%Y-%m-%d")
    mod_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""UPDATE tHR_Actions
                      SET EndDate = ?, ModifiedDate = ?,
                      ModifiedBy = 'Mass Upload'
                      WHERE ID = ?""", enddate, mod_date, row_id)
    

def close_job_details(row_id, stamp_date, cursor):
    """
    This function is closing existing record for terminated
    employee in tHR_JobDetails table
    """
    enddate = datetime.date(int(stamp_date[:4]),
                            int(stamp_date[5:7]), 01)
    enddate = enddate - datetime.timedelta(days=1)
    enddate = enddate.strftime("%Y-%m-%d")
    mod_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""UPDATE tHR_JobDetails
                      SET EndDate = ?, ModifiedDate = ?,
                      ModifiedBy = 'Mass Upload'
                      WHERE ID = ?""", enddate, mod_date, row_id)
    

def close_team_record(eeid, stamp_date, cursor):
    """
    This function is closing existingrecord for terminated
    employee in tHR_TeamMembers table, removing that person from
    the team
    """
    enddate = datetime.date(int(stamp_date[:4]),
                            int(stamp_date[5:7]), 01)
    enddate = enddate - datetime.timedelta(days=1)
    enddate = enddate.strftime("%Y-%m-%d")
    cursor.execute("""UPDATE tHR_TeamMembers
                      SET EndDate = ?
                      WHERE ID = ?""", enddate, row_id)
    

def remove_opsmru(eeid, cursor):
    """
    Deletes entry from tHR_OpsMRU removing Employee from MRU pool.
    """
    cursor.execute("""DELETE FROM tHR_OpsMRU
                      WHERE ID = ?""", eeid)
    
    
def insert_new_action(eeid, stamp_date, cursor):
    """
    Inserts new record with "Terminated status"
    """
    start_date = datetime.date(int(stamp_date[:4]),
                            int(stamp_date[5:7]), 01)
    mod_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    SQL = 
    cursor.execute("""INSERT INTO tHR_Actions
                     (EEID, ActionType, ReasonCode, StarDate, EndDate,
                      ModifiedDate, ModifiedBy, EmploymentStatus)
                      VALUES (?, 'Termination', 'Left Company', ?,
                      '9999-12-31', ?, 'Mass Upload', 'Terminated')""",
                   eeid, start_date, mod_date)


def check_for_action(eeid, stamp_date, cursor):
    """
    Dispatch function checking what kind of action should be performed
    on an employee.
    It will return action that needs to be taken by comparing status
    of given employee both in MiG and in BDW database.
    Switch class does the comparison.
    If BDW record is newer than MiG one, function will return before
    using switch class.
    """
    cursor.execute("""SELECT TOP 1 EmploymentStatus, StarDate
                      FROM tHR_Actions
                      WHERE EEID = ?
                      ORDER BY StarDate DESC, ID DESC""", eeid)
    result = cursor.fetchone()

    db_date = datetime.date(int(result[1][:4]), int(result[1][5:7]),
                            int(result[1][8:10]))

    stamp_date = datetime.date(int(stamp_date[:4]),
                               int(stamp_date[5:7]), 01)
    if db_date > stamp_date:
        return 'End'

    eestat = row[10]

   
    class switch(object):
        """
        Hacked switch statement - thanks Aaron!
        Class is comparing statuses from MiG (eestat) and BDW (supplied
        value). If Employee is:
        *Active in both -> regular update to be conducted
        *Active in BDW, LoA or LwP in MiG -> LoA or LwP action
        *Terminated in BDW, Active in MiG -> rehire action to be conducted
        *LoA/LwP in BDW, active in DBW -> return from LoA action
        *LwP in BDW, LoA in MiG -> LoA Action
        """
        def __init__(self):
            self.value = eestat
        def __getitem__(self, index):
            try:
                return getattr(self, "case_"+index)()
            except AttributeError:
                return 'End'
            
        def case_Active(self):
            if eestat == 'Active':
                return 'update'
            elif eestat == 'Leave of Absence': 
                return 'LoA'
            elif eestat == 'Leave With Pay':
                return 'LwP'
            else:
                return 'End'

        def case_Terminated(self):
            if eestat == 'Active':
                return 'rehire'
            else:
                return 'End'

        def case_LoA(self):
            if eestat == 'Active':
                return 'retFromLoA'
            else:
                return 'End'

        def case_LwP(self):
            if eestat == 'Active':
                return 'retFromLoA'
            elif eestat == 'Leave of Absence':
                return 'LoA'
            else:
                return 'End'

    return switch()[result[0]]
    
if __name__ == '__main__':
    """
    Program entry point.
    Command line argument should contain a date in YYYY-MM-DD format
    """
    main(sys.argv[1:])


def main(argv):
    """
    Task dispatcher function. Launches all the tasks one by one.
    """

    
    #Open file and load contents to memory
    openfile_and_preload()
    #build a list of terminations
    check_terminations()
    #terminate employees from the list
    terminate_employees(argv[0])
    
    for row in ROWS_TO_PROCESS:
        with get_connection() as cursor:
            action = check_for_action(row[1], argv[0], cursor)

            class actionswitch(object):             
                """
                This class takes action value and basing on it's
                content dispatches a job to correct chain of tasks
                to perform update, rehire, LoA, LwP or returnFromLoA
                """        
                def __getitem__(self, index):
                    return getattr(self, "case_"+index)()
                def case_update(self):
                    #run update functions
                    return none
                def case_rehire(self):
                    #run rehire actions
                    return none
                def case_LoA(self):
                    #run LoA
                    return none
                def case_LwP(self):
                    #run LwP
                    return none
                def case_retFromLoA(self):
                    #return from LoA
                    return none

            actionswitch()[action]
