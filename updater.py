import csv
import pyodbc
import sys
import datetime
from contextlib import contextmanager

MIG_IDS = {}
TERMINATION_LIST = []


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
    openfile_and_preload()
    check_terminations()
    terminate_employees(argv[0])
