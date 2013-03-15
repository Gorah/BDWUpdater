import csv
import pyodbc
import sys
import datetime
import re
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
            #check to not do double terminations (user made terms)
            if not already_terminated(eeid, cursor):
                row_id = find_actions_record(eeid, cursor)
                close_actions_record(row_id, stamp_date, cursor)
                insert_new_action(eeid, stamp_date, cursor,
                                  'Termination', 'Left Company',
                                  'Terminated')
                row_id = find_job_details_record(eeid, cursor)
                close_job_details(eeid, stamp_date, cursor)
                row_id = find_team_record(eeid, cursor)
                close_team_record(eeid, stamp_date, cursor)
                remove_opsmru(eeid, cursor)           
            

def already_terminated(eeid, cursor):
    cursor.execute("""SELECT Top 1 EmploymentStatus
                      FROM tHR_Actions
                      WHERE EEID = ?
                      ORDER BY StarDate, ID DESC""", eeid)
    result = cursor.fetchone()
    return result[0] == 'Terminated'
    
                
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
    
    
def insert_new_action(eeid, stamp_date, cursor, a_type, reason, estat):
    """
    Inserts new record to tHR_Actions table
    """
    start_date = datetime.date(int(stamp_date[:4]),
                            int(stamp_date[5:7]), 01)
    mod_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""INSERT INTO tHR_Actions
                     (EEID, ActionType, ReasonCode, StarDate, EndDate,
                      ModifiedDate, ModifiedBy, EmploymentStatus)
                      VALUES (?, ?, ?, ?,
                      '9999-12-31', ?, 'Mass Upload', ?)""",
                   eeid, a_type, reason, start_date, mod_date, estat)

    
def insert_new_job(row, stamp_date, cursor):
    """
    Inserts a new row into tHR_JobDetails table
    """
    if row[11] != 1:
        fpt = 'Part Time'
    else:
        fpt = 'Full Time'

    if row[0] == 'Regular':
        wrk_contr = 'Permanent'
    else:
        wrk_contr = row[0]
        
    start_date = datetime.date(int(stamp_date[:4]),
                               int(stamp_date[5:7]), 01)
    mod_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""INSERT INTO tHR_JobDetails (EEID, Project,
                   WorkContractType, CostCenter, FTE, FullPartTime,
                   ModifiedBy, StartDate, EndDate, ModifiedDate)
                   VALUES (?, ?, ?, ?, ?, ?, 'Mass Upload', ?,
                   '9999-12-31', ?)""", row[1], row[7], wrk_contr,
                   row[5], row[11], fpt, start_date, mod_date)

    
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

    db_date = datetime.datetime.strptime(result[1], "%Y-%m-%d")

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


def changeeedetails(row, cursor):
    """
    This function checks if there was a change in personal information
    and if yes, it performs update on employee record
    """
    if detailschanged(row, cursor):
        cursor.execute("""UPDATE tHR_Employee
                          SET FirstName = ?, LastName = ?, Email = ?
                          WHERE ID = ?""", row[3], row[2], row[4],
                       row[1])

        
def detailschanged(row, cursor):
    """
    This function is checking if any personal information for employee
    has been changed in MiG. If so, function returns True.
    """
    cursor.execute("""SELECT Count(ID)
                      WHERE ID = ?, FirstName = ?, LastName = ?,
                      Email = ?""", row[1], row[3], row[2], row[4])
    result = cursor.fetchone()
    return not result[0] == 1:


def changeMRU(eeid, MRU, cursor, mode):
    """
    changes MRU note in tHR_OpsMRU. If there was an entry in the
    table, it gets updated. Otherwise new row is inserted.
    """
    if is_in_OpsMRU(eeid, cursor) and mode == 'change':
        cursor.execute("""UPDATE tHR_OpsMRU
                      SET MRU = ?
                      WHERE ID = ?""", MRU, eeid)
    else:
        cursor.execute("""INSERT INTO tHR_OpsMRU (ID, MRU)
                          VALUES (?, ?)""", eeid, MRU)    
    

def changejob(row, stamp_date, cursor):
    """
    This function checks if there was a change in job for employee
    and if yes, it performs update on employee record
    """
    row_to_change = jobchanged(row, stamp_date, cursor)
    if row_to_change:
        close_actions_record(find_actions_record(row[1], cursor),
                             stamp_date, cursor)
        close_job_details(row_to_change, stamp_date, cursor)
        insert_new_job(row, stamp_date, cursor)
        insert_new_action(row[1], stamp_date, cursor, 'Job Change',
                          'Job Change', 'Active')
        if mruchanged(row[1], row[7], cursor):
            close_team_record(row[1], stamp_date, cursor)
            changeMRU(row[1], row[7], cursor, 'change')


def jobchanged(row, strftime, cursor):
    """
    This function is fetching most up to date record for an employee
    and checks for MRU, CC or FTE changes. If there was no changes or
    StartDate of the record is greater than stamp date of report,
    function will return False.
    If there was no record, function will return False and print
    notification about that fact.
    In case of newest record not matching to MiG data function returns
    row ID for further correction.
    """
    cursor.execute("""SELECT TOP 1 ID, Project, CostCenter, FTE, StartDate
                      FROM tHR_JobDetails
                      WHERE EEID =?
                      ORDER BY StartDate, ID DESC""", row[1])
    result = cursor.fetchone()
    if not result:
        print row[1] + " has no record in tHR_JobDetails! Skipped..."
        return False
    
    db_date = datetime.datetime.strptime(result[1], "%Y-%m-%d")

    stamp_date = datetime.date(int(stamp_date[:4]),
                               int(stamp_date[5:7]), 01)
    if db_date > stamp_date:
        return False
    elif result[1] != row[7] or result[2] != row[5] \
            or result[3] != row[11]:
        return result[0]


def insert_new_ee(row, cursor):
    """
    Inserts Personal info for a new employee
    """
    cursor.execute("""INSERT INTO tHR_Employee (ID, FirstName,
                      LastName, Email) VALUES""", row[1], row[3],
                      row[2], row[4])
     

def mruchanged(eeid, mru, cursor):
    """
    Function checking if there was a change in MRU for employee
    """
    cursor.execute("""SELECT TOP 1 Project
                      FROM tHR_JobDetails
                      WHERE EEID = ?
                      ORDER BY StartDate, ID DESC""", eeid)
    bdw_mru = cursor.fetchone()
    return not mru != bdw_mru[0]


def is_in_OpsMRU(eeid, cursor):
    """
    returns True if a row for given employee already existsin DB
    """
    cursor.execute("""SELECT Count(ID)
                      FROM tHR_OpsMRU
                      WHERE ID = ?""", eeid)
    in_mru = cursor.fetchone()
    return in_mru[0] == 1

def check_new_hire(eeid, cursor):
    """
    Returns True if employee from MiG is not in BDW, as this employee
    has to be hired in the system
    """
    cursor.execute("""SELECT Count(ID)
                      FROM tHR_Employee
                      WHERE ID = ?""", eeid)
    result = cursor.fetchone()
    return result[0] == 0
    
    
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
            if not check_new_hire(row[1], cursor):
                action = check_for_action(row[1], argv[0], cursor)
            else:
                action = 'Hire'
                
            class actionswitch(object):             
                """
                This class takes action value and basing on it's
                content dispatches a job to correct chain of tasks
                to perform update, rehire, LoA, LwP or returnFromLoA
                """        
                def __getitem__(self, index):
                    return getattr(self, "case_"+index)()
                def case_Hire(self):
                    insert_new_ee(row, cursor)
                    insert_new_action(row[1], stamp_date, cursor,
                                      'New Hire', 'New Hire',
                                      'Active')
                    insert_new_job(row, stamp_date, cursor)
                    changeMRU(row[1], row[7], cursor, 'add')
                def case_update(self):
                    changeeedetails(row, cursor)
                    changejob(row, stamp_date, cursor)
                def case_rehire(self):
                    close_actions_record(find_actions_record(row[1],
                                                             cursor),
                                         stamp_date, cursor)
                    insert_new_action(row[1], stamp_date, cursor,
                                      'Rehire', 'Rehire',
                                      'Active')
                    insert_new_job(row, stamp_date, cursor)
                    changeMRU(row[1], row[7], cursor, 'add')
                def case_LoA(self):
                    close_actions_record(find_actions_record(row[1],
                                                             cursor),
                                         stamp_date, cursor)
                    insert_new_action(row[1], stamp_date, cursor,
                                      'Leave of Absence',
                                      'Leave of Absence', 'LoA') 
                def case_LwP(self):
                     close_actions_record(find_actions_record(row[1],
                                                             cursor),
                                         stamp_date, cursor)
                     insert_new_action(row[1], stamp_date, cursor,
                                       'Leave with Pay',
                                       'Leave with Pay', 'LwP')
                def case_retFromLoA(self):
                    close_actions_record(find_actions_record(row[1],
                                                             cursor),
                                         stamp_date, cursor)
                     insert_new_action(row[1], stamp_date, cursor,
                                       'Return from LoA',
                                       'Return from LoA', 'Active')

            actionswitch()[action]


 if __name__ == '__main__':
    """
    Program entry point.
    Command line argument should contain a date in YYYY-MM-DD format
    """
    if len(sys.argv) == 1:
        print "Missing date, please pass it as an argument!"
        sys.exit()
    elif not re.match(r"\d{4}-\d{2}-\d{2}", sys.argv[1]):
        print "Incorrect date format - should be YYYY-MM-DD"
        sys.exit()
        
    main(sys.argv[1:])           
