import sys
sys.path.insert(0, "/home/Shared")
sys.path.insert(0, "/home/DB_model")

from jira_extractor import JiraIssues,  JiraDataByProject
from release_db_updater import UpdateReleaseDB
import urllib3
import pandas as pd
import pymysql
from utils import create_db_session, start_logging
import datetime
urllib3.disable_warnings()
pd.options.mode.chained_assignment = None



def main():
    pymysql.install_as_MySQLdb()
    logger, start_time = start_logging('update_db_from_jira')
    try:
        mode = "prod"
        Session, engine = create_db_session(mode)
        session = Session()
        jira_data_by_pr = JiraDataByProject(logger, session)
        project_data_mp = jira_data_by_pr.get_web_mp_data("DBIOSCA")
        project_data_web = jira_data_by_pr.get_web_mp_data("DBSBOLW")
        project_data_ufs = jira_data_by_pr.get_ufs_data("LINEUP")
        release_db = UpdateReleaseDB(logger, session)
        release_db.update_mp_web_db(project_data_mp)
        release_db.update_mp_web_db(project_data_web)
        release_db.update_ufs_db(project_data_ufs)
    except Exception as ex:
        print(ex)
    finally:
        update_time = datetime.datetime.now().replace(microsecond=0) - start_time
        logger.info(f"End of DB updating. Update time: {str(update_time)}")
        session.close()


if __name__ == '__main__':
    main()
