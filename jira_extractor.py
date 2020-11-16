import sys
sys.path.insert(0, "/home/Shared")
sys.path.insert(0, "/home/DB_model")

import unicodedata
import datetime
import re
import numpy as np
import pandas as pd
import os
import json
from dateutil.parser import parse
from mysql_orm import ReleaseRow, DORInformation
import pymysql
from utils import connect_jira, TextParser

class JiraIssues:
    def __init__(self, logger, session):
        self.logger = logger
        self.session = session
        self.engine = session.get_bind()
        self.date_now = datetime.date.today()  # datetime.datetime.now()
        pymysql.install_as_MySQLdb()
        try:
            self.jira = connect_jira()
        except Exception as ex:
            self.logger.error(f'__JiraIssues__Error {str(ex)} while connecting to Jira')
            raise
            # sys.exit(-1)


    def get_issues(self, issue_type, subtasksparents=[], epics=[], linkedIssues=[], project=''):
        issues = []
        chunk_size = 100
        maxresults = 500
        issuekeys_jql_lst = []
        project_jql = ''
        issue_type_jql = ''
        issuekeys = []

        if len(subtasksparents) > 0:
            issuekeys = subtasksparents
            issuefunction = 'subtasksOf'
        elif len(epics) > 0:
            issuekeys = epics
            issuefunction = 'issuesInEpics'
        elif len(linkedIssues) > 0:
            issuekeys = linkedIssues
            issuefunction = 'linkedIssuesOf'

        if project:
            project_jql = f' AND project={project}'

        if issue_type:
            issue_type_jql = f'issuetype={issue_type}'

        if len(issuekeys) > 0:
            issuekeys_chunks = [issuekeys[i:i + chunk_size] for i in range(0, len(issuekeys), chunk_size)]
            for chunk in issuekeys_chunks:
                issuekeys_str = ', '.join(chunk)
                issuekeys_jql = [f' AND issueFunction in {issuefunction}("key in ({issuekeys_str})")']
                issuekeys_jql_lst.extend(issuekeys_jql)
        else:
            issuekeys_jql_lst.extend(' ')
        # try:
        for issuekeys_jql in issuekeys_jql_lst:
            start = 0
            while True:
                attempt = 0
                while True:
                    try:
                        attempt +=1
                        issue_chunk = self.jira.search_issues(
                            f'{issue_type_jql}{project_jql}{issuekeys_jql} order by created desc', startAt=start,
                            maxResults=maxresults, validate_query=True,
                            fields="id,key,project,issuetype,description,summary,customfield_21100,comment,labels,parent,status,created,updated,issuelinks, customfield_10006, customfield_18606, customfield_16701, customfield_16700, assignee,customfield_18605",
                            json_result=True)
                        break
                    except Exception as ex:
                        print("We are here!!!")
                        if attempt <= 10:
                            self.logger.error(f'__JiraIssues__Error {str(ex)} while getting data from Jira, repeating...')
                        else:
                            self.logger.error(
                                f'__JiraIssues__Error {str(ex)} while getting data from Jira, too much attempts')
                            raise
                start += maxresults
                if len(issue_chunk['issues']) > 0:
                    issues.extend(issue_chunk['issues'])
                else:
                    break
        return issues


    def get_issues_by_query(self, query, fields=""):
        start = 0
        maxresults = 1000
        issues = []
        try:
            while True:
                issue_chunk = self.jira.search_issues(f'{query} order by created desc', startAt=start, maxResults=maxresults,
                                                 validate_query=True, fields=fields, json_result=True) # fields="key,priority",
                start += maxresults
                if len(issue_chunk['issues']) > 0:
                    issues.extend(issue_chunk['issues'])
                else:
                    break
            return issues
        except Exception as ex:
            self.logger.error(f'__JiraIssues__Error {str(ex)} while getting data from Jira')
            raise
            # sys.exit(-1)


class JiraDataByProject(JiraIssues):
    def __init__(self, logger, session):
        super().__init__(logger, session)
        self.platform_dict, self.df_id_releases = self.get_release_info()
        self.df_dor_info, self.dor_labels_dict = self.get_dor_info()


    def get_dor_info(self):
        path = os.path.dirname(os.path.realpath(__file__))
        # PATH = r'%s' % os.getcwd().replace('\\', '/')
        PATH_TO_JSON = path + '/dor_types.json'
        print(f"path to dor_types.json:{PATH_TO_JSON}")

        with open(PATH_TO_JSON) as json_file:
            data = json.load(json_file)
        df_dor_info = pd.DataFrame.from_records(data['DOR-info'])
        df_active_dortypes =  pd.read_sql(self.session.query(DORInformation.dor_type_id).filter_by(active='true').statement, self.engine)
        df_dor_info = pd.merge(df_dor_info, df_active_dortypes, on='dor_type_id', how="inner")
        all_jira_lables = list(set(df_dor_info['label'].to_list()))
        dor_labels_dict = {label.lower(): label for label in all_jira_lables}
        return df_dor_info, dor_labels_dict

    def correct_release_label(self, release_label):
        release_label = [release.lower() for release in release_label]
        for release in release_label:
            if any(item in release for item in ["new", "major", "patch", "hotfix", "mass"]) == True:
                release_label = [key for key in ["new", "major", "patch", "hotfix", "mass"] if key in release]
                release_label = release_label[0]
                return release_label
        self.logger.info(f"__JiraDataByProject__info: Label doesn't exist for ufs release label {release_label}")
        release_label = ""
        return release_label

    def get_channel(self, release_label ):
        channel = ""
        release_label = [release.lower() for release in release_label]
        if "сбол.про" in release_label:
            channel = "sbolpro"
        return channel




    def correct_dor_label(self, dor_label, label_lst=[]):

        if str(dor_label) == 'nan' or dor_label == []:
            return ""

        if isinstance(dor_label, str)   == True:
            dor_label = [dor_label]

        dor_label = [label.lower() for label in dor_label]

        if any(item in dor_label for item in self.dor_labels_dict.keys()) == True:
            dor_label = [self.dor_labels_dict[key] for key in self.dor_labels_dict.keys() if key in dor_label]
            dor_label = dor_label[0]
        else:
            if dor_label not in label_lst:
                label_lst.append(dor_label)
                self.logger.info(f"__JiraDataByProject__info: Label doesn't exist for jira label {dor_label}")
            dor_label = ""
        return dor_label



    @staticmethod
    def change_date_format(date_str):
        if str(date_str) == 'nan':
            return date_str
        try:
            date_obj = parse(date_str)
        except Exception as ex:
            date_obj = parse('2000-01-01')

        date_str = str(date_obj.date()) + ' ' + str(date_obj.time())
        return date_str




    def get_release_info(self):
        platform_dict = {'android': 'android', 'ios': 'ios', 'releases': 'ufs', 'web': 'web'}
        try:
            df_id_releases = pd.read_sql(self.session.query(ReleaseRow).statement, self.engine)
            df_id_releases.rename(columns={'release_id': 'id', 'jira_key': 'key'}, inplace=True)
            df_id_releases = df_id_releases[['id', 'key' ]]
            return platform_dict, df_id_releases
        except Exception as err:
            self.logger.error(f'__JiraIssues__Error {str(err)} while getting data from Jira')
            raise


    #Подтянем наименование платфоры из json
    def correct_platform_name(self, platform_name):
        platform_name = [platform.lower() for platform in platform_name]
        if any(item in platform_name for item in self.platform_dict.keys()) == True:
            platform_name = [self.platform_dict[key] for key in self.platform_dict.keys() if key in platform_name]
            platform_name = platform_name[0]
        else:
            self.logger.error(f"__JiraDataByProject__error: Platform doesn't exist for jira label {platform_name}")
        return platform_name


    @staticmethod
    def has_lables(text, *lables):
        if any(label in lables for label in text):
            return True
        else:
            return False

    @staticmethod
    def str_to_date(date_time_str):
        try:
            date_time_str = date_time_str[0:10]
            date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d').date()
        except Exception:
            date_time_obj = datetime.datetime.strptime('0001-01-01', '%Y-%m-%d').date()
        return date_time_obj

    @staticmethod
    def check_date(date_str):
        try:
            date_time_str = date_str[0:10]
            date_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d').date()

            # datetime_obj = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
            # date_obj = datetime_obj.strftime("%Y-%d-%m %H:%M:%S")
            date_str = date_str[0:10]
            # date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d ').date()
            date_str = str(date_obj)
        except Exception:
            date_str = ""
        return date_str

    @staticmethod
    def list_to_str(elem):
        if isinstance(elem, list) == True and len(elem) == 1:
            elem = str(elem[0])
        elif isinstance(elem, list) == True and len(elem) > 1:
            elem = str(elem)
        return elem

    @staticmethod
    def fill_release_num(release_id):
        release_number = 0
        release_number = re.sub(r"\D", "", release_id)
        if len(release_number) > 0:
            release_number = int(release_number)
        return release_number

    def fill_dor_type(self, project_data, RELEASE_TYPE):
        project_data = pd.merge(project_data, self.df_dor_info,
                                left_on=[f'project_{RELEASE_TYPE}', 'fields.labels_subtasks'],
                                right_on=['project', 'label'], how="left")
        project_data.rename(columns={'dor_type_id': 'dor_type_id_subtasks'}, inplace=True)
        project_data = pd.merge(project_data, self.df_dor_info, left_on=[f'project_{RELEASE_TYPE}', 'fields.labels_UXUI'],
                                right_on=['project', 'label'], how="left")
        project_data.rename(columns={'dor_type_id': 'dor_type_id_UXUI'}, inplace=True)
        return project_data

    @staticmethod
    def fill_project(df_tab):
        df_tab['project'] = ''
        df_tab['project'] = np.where(df_tab['fields.project.key'] == 'LINEUP', 'ufs', df_tab['project'])
        df_tab['project'] = np.where(df_tab['fields.project.key'] == 'DBIOSCA', 'mob', df_tab['project'])
        df_tab['project'] = np.where(df_tab['fields.project.key'] == 'DBSBOLW', 'web', df_tab['project'])
        return df_tab

    # Сгруппируем  для удобства в один столбец inwardIssues и outwardIssues
    def collect_linked_issues(self, df_tab):
        try:
            if 'linked_outwardIssue.id' in df_tab.columns and 'linked_inwardIssue.id' in df_tab.columns:
                df_tab['linked_Issue.id'] = np.where(pd.isna(df_tab['linked_inwardIssue.id']),
                                                     df_tab['linked_outwardIssue.id'], df_tab['linked_inwardIssue.id'])
                df_tab['type'] = np.where(pd.isna(df_tab['linked_inwardIssue.id']), 'outward', 'inward')
                df_tab['linked_Issue.key'] = np.where(pd.isna(df_tab['linked_inwardIssue.id']),
                                                      df_tab['linked_outwardIssue.key'], df_tab['linked_inwardIssue.key'])
                df_tab['linked_Issue.fields.issuetype.id'] = np.where(pd.isna(df_tab['linked_inwardIssue.id']),
                                                                      df_tab['linked_outwardIssue.fields.issuetype.id'],
                                                                      df_tab['linked_inwardIssue.fields.issuetype.id'])
                df_tab['linked_Issue.fields.issuetype.name'] = np.where(pd.isna(df_tab['linked_inwardIssue.id']),
                                                                        df_tab['linked_outwardIssue.fields.issuetype.name'],
                                                                        df_tab['linked_inwardIssue.fields.issuetype.name'])
                df_tab['linked_Issue.fields.status.name'] = np.where(pd.isna(df_tab['linked_inwardIssue.id']),
                                                                     df_tab['linked_outwardIssue.fields.status.name'],
                                                                     df_tab['linked_inwardIssue.fields.status.name'])
            elif 'linked_outwardIssue.id' in df_tab.columns:
                df_tab['linked_Issue.id'] = df_tab['linked_outwardIssue.id']
                df_tab['type'] = 'outward'
                df_tab['linked_Issue.key'] =  df_tab['linked_outwardIssue.key']
                df_tab['linked_Issue.fields.issuetype.id'] = df_tab['linked_outwardIssue.fields.issuetype.id']
                df_tab['linked_Issue.fields.issuetype.name'] = df_tab['linked_outwardIssue.fields.issuetype.name']
                df_tab['linked_Issue.fields.status.name'] = df_tab['linked_outwardIssue.fields.status.name']

            elif 'linked_inwardIssue.id' in df_tab.columns:
                df_tab['linked_Issue.id'] = df_tab['linked_inwardIssue.id']
                df_tab['type'] = 'inward'
                df_tab['linked_Issue.key'] =  df_tab['linked_inwardIssue.key']
                df_tab['linked_Issue.fields.issuetype.id'] = df_tab['linked_inwardIssue.fields.issuetype.id']
                df_tab['linked_Issue.fields.issuetype.name'] = df_tab['linked_inwardIssue.fields.issuetype.name']
                df_tab['linked_Issue.fields.status.name'] = df_tab['linked_inwardIssue.fields.status.name']
            else:
                df_tab['linked_Issue.id'] = ""
                df_tab['type']= ""
                df_tab['linked_Issue.key']= ""
                df_tab['linked_Issue.fields.issuetype.id']= ""
                df_tab['linked_Issue.fields.issuetype.name']= ""
                df_tab['linked_Issue.fields.status.name']= ""
        except Exception as ex:
            print(ex)
            self.logger.error( f"__JiraDataByProject__error: {ex} while collecting linked issues")
        return df_tab

    def remove_wrong_story_links(self, project_data, release_type):
        release_id = 'id_' + release_type
        release_platform = 'fields.labels_' + release_type
        if 'key_stories' not in project_data.columns:
            project_data['key_stories'] = ""
        df_stories = project_data[[release_id, release_platform, 'key_stories', 'key_applications', 'release_number']]
        df_stories['release_number'] = df_stories['release_number'].astype(str)
        df_stories = df_stories.sort_values(
            by=[release_id, release_platform, 'key_stories', 'key_applications', 'release_number'])
        df_stories = df_stories.drop_duplicates(
            subset=[release_id, release_platform, 'key_stories', 'key_applications', 'release_number'])
        df_wrong_stories = df_stories
        df_wrong_stories['release_count'] = 1
        df_wrong_stories = df_wrong_stories.groupby(
            [release_platform, 'key_stories', 'key_applications']).sum().reset_index()
        df_wrong_stories = df_wrong_stories[df_wrong_stories['release_count'] != 1]
        if release_type == 'release':
            for ind, row in df_wrong_stories.iterrows():
                print(
                    f"__JiraDataByProject__error: Story {row['key_stories']} has {row['release_count']} links to {row[release_platform]} releases")
                self.logger.error(f"__JiraDataByProject__error: Story {row['key_stories']} has {row['release_count']} links to {row[release_platform]} releases")
        else:
            for ind, row in df_wrong_stories.iterrows():
                print(
                    f"__JiraDataByProject__error: Application {row['key_applications']} has {row['release_count']} links to {row[release_platform]} releases")
                self.logger.error(f"__JiraDataByProject__error: Application {row['key_applications']}  has {row['release_count']} links to {row[release_platform]} releases")

        df_stories = df_stories.sort_values(by=[release_platform, 'key_stories', 'key_applications', 'release_number'])
        df_stories = df_stories.drop_duplicates(subset=[release_platform, 'key_stories', 'key_applications'],
                                                keep="last")
        df_stories = df_stories[[release_id, release_platform, 'key_stories', 'key_applications']]
        project_data = pd.merge(project_data, df_stories,
                                on=[release_id, release_platform, 'key_stories', 'key_applications'], how="inner")
        return project_data


    def get_releases(self, project):
        release_issues = self.get_issues(project=project, issue_type="15305")
        df_releases = pd.json_normalize(release_issues,
                                        meta=['id', 'key', ['fields','project', 'key'], ['fields', 'labels'], ['fields', 'summary'],
                                              ['fields', 'customfield_18606'], ['fields', 'customfield_18605'],
                                              ['fields', 'status', 'name'], ['fields', 'status', 'id'],
                                              ['fields', 'issuetype', 'id'], ['fields', 'issuetype', 'name']],
                                        record_path=['fields', 'issuelinks'], record_prefix='linked_',
                                        errors='ignore')
        df_releases = self.collect_linked_issues(df_releases)
        df_releases = df_releases[
            ['id', 'key', 'type', 'fields.project.key', 'fields.labels', 'fields.summary', 'fields.customfield_18606',
             'fields.customfield_18605', 'fields.status.name', 'fields.status.id', 'fields.issuetype.id',
             'fields.issuetype.name', 'linked_Issue.id', 'linked_Issue.key', 'linked_Issue.fields.issuetype.id',
             'linked_Issue.fields.issuetype.name']]
        # print(self.date_now)
        # print(type(self.date_now))
        # Выберем только те релизы, у которых Начало внедрения (план) еще не наступило
        df_releases['customfield_18606_date_obj'] = df_releases['fields.customfield_18606'].apply(self.str_to_date)
        # print(df_releases['customfield_18606_date_obj'])
        # print(type(df_releases['customfield_18606_date_obj'][0]))
        df_releases = df_releases[df_releases['customfield_18606_date_obj'] >= self.date_now]
        df_releases['customfield_18605_date_obj'] = df_releases['fields.customfield_18605'].apply(self.str_to_date)
        df_releases.rename(columns={'id': 'jira_id'}, inplace=True)
        df_releases = pd.merge(df_releases, self.df_id_releases, on='key', how="inner")
        df_releases['fields.labels'] = df_releases['fields.labels'].apply(self.correct_platform_name)
        df_releases['fields.labels'] = df_releases['fields.labels'].apply(self.list_to_str)
        df_releases = self.fill_project(df_releases)
        df_releases.columns = [x + '_releases' for x in df_releases.columns]
        releases = [issue['key'] for issue in release_issues]
        self.logger.info(f'__JiraIssues__Info: Uploaded {len(releases)} releases from Jira by project {project}')
        return df_releases, releases

    def get_stories(self, releases):
        story_issues = self.get_issues(issue_type="10001", linkedIssues=releases)
        df_stories = pd.json_normalize(story_issues, meta=['id', 'key', ['fields', 'summary'], ['fields', 'labels'],
                                                           ['fields', 'issuetype', 'id'],
                                                           ['fields', 'issuetype', 'name'],
                                                           ['fields', 'status', 'id'],
                                                           ['fields', 'status', 'name']],
                                       record_path=['fields', 'issuelinks'], record_prefix='linked_')
        df_stories = self.collect_linked_issues(df_stories)
        df_stories = df_stories[
            ['id', 'key', 'type', 'fields.summary', 'fields.labels', 'fields.status.name', 'fields.status.id',
             'fields.issuetype.id', 'fields.issuetype.name', 'linked_Issue.id', 'linked_Issue.key',
             'linked_Issue.fields.issuetype.id', 'linked_Issue.fields.issuetype.name',
             'linked_Issue.fields.status.name']]
        df_stories.columns = [x + '_stories' for x in df_stories.columns]
        stories = [story['key'] for story in story_issues]
        self.logger.info(f'__JiraIssues__Info: Uploaded {len(stories)} stories from Jira')
        return df_stories, stories

    # application это инициатива
    def get_applications(self, stories):
        application_issues = self.get_issues(issue_type="10749", linkedIssues=stories)
        df_app_links = pd.json_normalize(application_issues, meta=['id', 'key'],
                                         record_path=['fields', 'issuelinks'], record_prefix='linked_')
        df_app = pd.json_normalize(application_issues,
                                   meta=['id', 'key', ['fields', 'labels'], ['fields', 'created'], ['fields', 'issuetype', 'id'],
                                         ['fields', 'issuetype', 'name'], ['fields', 'status', 'id'],
                                         ['fields', 'status', 'name']])
        df_applications = pd.merge(df_app, df_app_links, on=['id', 'key'], how="inner")
        df_applications = self.collect_linked_issues(df_applications)
        df_applications = df_applications[
            ['id', 'key', 'type', 'fields.labels', 'fields.created', 'fields.updated', 'fields.status.name', 'fields.status.id', 'fields.issuetype.id',
             'fields.issuetype.name', 'linked_Issue.id', 'linked_Issue.key', 'linked_Issue.fields.issuetype.id',
             'linked_Issue.fields.issuetype.name']]
        df_applications['fields.created'] = df_applications['fields.created'].astype(str)
        df_applications['fields.updated'] = df_applications['fields.updated'].astype(str)
        df_applications['fields.created'] = df_applications['fields.created'].apply(self.check_date)
        df_applications['fields.updated'] = df_applications['fields.updated'].apply(self.check_date)
        df_applications['fields.labels'] = df_applications['fields.labels'].apply(self.correct_platform_name)
        df_applications.columns = [x + '_applications' for x in df_applications.columns]
        applications = [application['key'] for application in application_issues]
        self.logger.info(f'__JiraIssues__Info: Uploaded {len(applications)} applications from Jira')
        return df_applications, applications

    def get_uxui(self, applications):
        uxui_issues = self.get_issues(issue_type="14705", linkedIssues=applications)

        df_uxui = pd.json_normalize(uxui_issues)
        df_uxui_links = pd.json_normalize(uxui_issues, meta=['id', 'key'], record_path=['fields', 'issuelinks'],
                                          record_prefix='linked_')
        df_uxui_links = pd.merge(df_uxui, df_uxui_links, on=['id', 'key'], how="inner")
        #         df_uxui_links = pd.json_normalize(uxui_issues, meta = ['id', 'key', ['fields', 'status', 'name'], ['fields', 'labels' ], ['fields', 'description'], ['fields', 'updated'] ], record_path = ['fields', 'issuelinks'], record_prefix='linked_', errors='ignore')
        df_uxui_links = self.collect_linked_issues(df_uxui_links)
        df_uxui_links = df_uxui_links[['linked_Issue.id', 'type', 'fields.description', 'linked_Issue.key',
                                       'linked_Issue.fields.issuetype.id', 'linked_Issue.fields.issuetype.name',
                                       'linked_Issue.fields.status.name', 'id', 'key', 'fields.status.name',
                                       'fields.labels', 'fields.updated', 'fields.assignee.key',
                                       'fields.assignee.emailAddress', 'fields.assignee.displayName']]
        df_uxui_links['fields.description'] = df_uxui_links['fields.description'].astype(str)
        df_uxui_links = self.fill_dor_content(df_uxui_links)
        df_uxui_links = self.map_dor_status(df_uxui_links)
        label_lst = []
        # print('df_uxui_links:')
        # print(df_uxui_links[['key', 'fields.labels']])
        df_uxui_links['fields.labels'] = np.where(df_uxui_links['key'].str.startswith('DBPUXUI') == True,
                                                   ['DBPUXUI'], df_uxui_links['fields.labels'])
        df_uxui_links['fields.labels'] = np.where(df_uxui_links['key'].str.startswith('COREDSGN') == True,
                                                   ['COREDSGN'], df_uxui_links['fields.labels'])
        df_uxui_links['fields.labels'] = df_uxui_links['fields.labels'].apply(self.correct_dor_label, args=[label_lst])
        df_uxui_links['fields.updated'] = df_uxui_links['fields.updated'].apply(self.change_date_format)
        # df_uxui_links['fields.labels'] = df_uxui_links['fields.labels'].astype(str)
        df_uxui_links.columns = [x + '_UXUI' for x in df_uxui_links.columns]
        df_uxui_links = df_uxui_links[df_uxui_links['linked_Issue.fields.issuetype.id_UXUI'] == '10749']
        uxui = [uxui['key'] for uxui in uxui_issues]
        self.logger.info(f'__JiraIssues__Info: Uploaded {len(uxui)} uxui from Jira')
        return df_uxui_links

    def get_subtasks(self, applications):
        subtask_issues = self.get_issues(issue_type="5", subtasksparents=applications)
        df_subtasks = pd.json_normalize(subtask_issues)
        df_subtasks = df_subtasks[
            ['key', 'fields.parent.key', 'fields.status.name', 'fields.labels', 'fields.description',
             'fields.updated', 'fields.assignee.key', 'fields.assignee.emailAddress',
             'fields.assignee.displayName']]
        df_subtasks['fields.description'] = df_subtasks['fields.description'].astype(str)
        df_subtasks = self.fill_dor_content(df_subtasks)
        df_subtasks = self.map_dor_status(df_subtasks)
        label_lst = []
        # print(f'df_subtasks:')
        # print(df_subtasks[['key', 'fields.labels']])
        df_subtasks['fields.labels'] = df_subtasks['fields.labels'].apply(self.correct_dor_label, args=[label_lst])
        # df_subtasks['fields.labels'] = df_subtasks['fields.labels'].astype(str)
        df_subtasks['fields.updated'] = df_subtasks['fields.updated'].apply(self.change_date_format)
        df_subtasks.columns = [x + '_subtasks' for x in df_subtasks.columns]
        subtasks = [subtask['key'] for subtask in subtask_issues]
        self.logger.info(f'__JiraIssues__Info: Uploaded {len(subtasks)} subtasks from Jira')
        return df_subtasks

    def get_epics(self, project):
        epic_issues = self.get_issues(project=project, issue_type="10000")
        df_epics = pd.json_normalize(epic_issues)
        df_epics = df_epics[
            ['id', 'key', 'fields.project.key', 'fields.summary', 'fields.issuetype.id', 'fields.issuetype.name', 'fields.labels',
             'fields.status.name', 'fields.customfield_16701', 'fields.customfield_16700']]
        # Оставим только те эпики, у которых запланированное окончание (Planned End) еще не наступило
        df_epics['customfield_16701_date_obj'] = df_epics['fields.customfield_16701'].apply(self.str_to_date)
        # print(type(df_epics['customfield_16701_date_obj'][0]))
        df_epics = df_epics[df_epics['customfield_16701_date_obj'] >= self.date_now]
        df_epics.rename(columns={'id': 'jira_id'}, inplace=True)
        df_epics = pd.merge(df_epics, self.df_id_releases, on='key', how="inner")
        df_epics['fields.labels'] = df_epics['fields.labels'].apply(self.correct_platform_name)
        df_epics['fields.labels'] = df_epics['fields.labels'].apply(self.list_to_str)
        df_epics = self.fill_project(df_epics)
        df_epics.columns = [x + '_epics' for x in df_epics.columns]
        epics = [epic_issue['key'] for epic_issue in epic_issues]
        self.logger.info(f'__JiraIssues__Info: Uploaded {len(epics)} epics from Jira by project {project}')
        return df_epics, epics

    def get_issues_in_epics(self, epics):
        issues_in_epic = self.get_issues(issue_type="10749", epics=epics)
        df_app_in_epic = pd.json_normalize(issues_in_epic)
        df_app_in_epic_links = pd.json_normalize(issues_in_epic, meta=['key'], record_path=['fields', 'issuelinks'],
                                                 record_prefix='linked_')
        df_app_in_epic = pd.merge(df_app_in_epic, df_app_in_epic_links, on='key', how="left")
        df_app_in_epic = self.collect_linked_issues(df_app_in_epic)
        df_app_in_epic = df_app_in_epic[
            ['linked_id', 'key', 'fields.created', 'fields.updated', 'linked_self', 'linked_type.id', 'linked_type.name', 'fields.summary',
             'fields.issuetype.id', 'fields.issuetype.name', 'fields.customfield_10006', 'fields.description',
             'fields.status.name', 'fields.labels', 'linked_Issue.id', 'type', 'linked_Issue.key',
             'linked_Issue.fields.issuetype.id', 'linked_Issue.fields.issuetype.name',
             'linked_Issue.fields.status.name']]
        df_app_in_epic['fields.created'] = df_app_in_epic['fields.created'].astype(str)
        df_app_in_epic['fields.updated'] = df_app_in_epic['fields.updated'].astype(str)
        df_app_in_epic['fields.created'] = df_app_in_epic['fields.created'].apply(self.check_date)
        df_app_in_epic['fields.updated'] = df_app_in_epic['fields.updated'].apply(self.check_date)
        df_app_in_epic.columns = [x + '_applications' for x in df_app_in_epic.columns]
        df_app_in_epic['channel_applications'] = df_app_in_epic['fields.labels_applications'].apply(self.get_channel)
        df_app_in_epic['fields.labels_applications']  = df_app_in_epic['fields.labels_applications'].apply(self.correct_release_label)
        #         applications = list(set(df_app_in_epic['key_applications'].to_list()))
        applications = [application['key'] for application in issues_in_epic]
        self.logger.info(f'__JiraIssues__Info: Uploaded {len(applications)} applications from Jira by epics')
        return df_app_in_epic, applications

    def get_web_mp_data(self, project):
        df_releases, releases = self.get_releases(project)
        df_stories, stories = self.get_stories(releases)
        df_applications, applications = self.get_applications(stories)
        # self.reconnect_jira()
        # print('ура')
        df_subtasks = self.get_subtasks(applications)
        df_uxui = self.get_uxui(applications)
        df_rel_st = pd.merge(df_releases, df_stories,
                             left_on=['linked_Issue.id_releases', 'linked_Issue.fields.issuetype.id_releases'],
                             right_on=['id_stories', 'fields.issuetype.id_stories'], how="inner")
        df_project_data = pd.merge(df_rel_st, df_applications,
                                   left_on=['linked_Issue.id_stories', 'linked_Issue.fields.issuetype.id_stories'],
                                   right_on=['id_applications', 'fields.issuetype.id_applications'], how="inner")
        df_project_data['release_number'] = df_project_data['id_releases'].apply(self.fill_release_num)
        df_project_data['fields.labels_applications'] = df_project_data['fields.labels_applications'].astype(str)
        df_project_data = df_project_data[
            df_project_data['fields.labels_applications'] == df_project_data['fields.labels_releases']]

        df_project_data = pd.merge(df_project_data, df_subtasks, left_on='key_applications',
                                   right_on='fields.parent.key_subtasks', how="left")
        df_project_data = pd.merge(df_project_data, df_uxui, left_on='key_applications',
                                   right_on='linked_Issue.key_UXUI', how="left")
        df_project_data = self.fill_dor_type(df_project_data, 'releases')
        df_project_data = self.remove_wrong_story_links(df_project_data, 'releases')
        return df_project_data

    def get_ufs_data(self, project):
        df_epics, epics = self.get_epics(project)
        df_app_in_epic, applications = self.get_issues_in_epics(epics)
        # self.reconnect_jira()
        df_subtasks = self.get_subtasks(applications)
        df_uxui = self.get_uxui(applications)
        df_project_data = pd.merge(df_epics, df_app_in_epic, left_on='key_epics',
                                   right_on='fields.customfield_10006_applications', how="inner")
        df_project_data['release_number'] = df_project_data['id_epics'].apply(self.fill_release_num)
        df_project_data = pd.merge(df_project_data, df_subtasks, left_on='key_applications',
                                   right_on='fields.parent.key_subtasks', how="left")
        df_project_data = pd.merge(df_project_data, df_uxui, left_on='key_applications',
                                   right_on='linked_Issue.key_UXUI', how="left")
        df_project_data = self.fill_dor_type(df_project_data, 'epics')
        df_project_data['fields.labels_applications'] = df_project_data['fields.labels_applications'].astype(str)
        df_project_data['id_releases'] = ''
        df_project_data['key_releases'] = ''
        df_project_data = self.remove_wrong_story_links(df_project_data, 'epics')
        return df_project_data

    def fill_dor_content(self, df_tab):
        df_tab['fields.description'] = df_tab['fields.description'].apply(TextParser.normalize_text)
        df_tab['fields.description'] = df_tab['fields.description'].apply(TextParser.remove_tables)
        df_tab['urls'] = df_tab['fields.description'].apply(TextParser.extract_url)
        df_tab['fields.description'] = df_tab['fields.description'].apply(TextParser.remove_extra_symbols)
        df_tab['fields.description_wo_url'] = df_tab['fields.description'].apply(TextParser.remove_url)
        return df_tab

    @staticmethod
    def map_dor_status(df_tab):
        to_fill_lst = ['TO DO', 'OPEN']
        in_progress_lst = ['IN PROGRESS', 'AWAITING REVIEW', 'IN REVIEW', 'REVIEW IN PROGRESS', 'WAITING',
                           'READY TO START', 'WAITING FOR APPROVAL', 'UX_REVIEW', 'DESIGN']
        to_fix_lst = ['TESTING', 'NEED INFO', 'REOPENED', 'NEED TO UPDATE']
        complete_lst = ['DONE', 'RESOLVED', 'CLOSED', 'APPROVED', 'PILOT', 'ЗАКРЫТ', 'ВЫПОЛНЕНО', 'REJECTED']
        ignore_lst = ['CANCELED', 'CANCELLED', 'READY FOR QA']

        df_tab['fields.status.name'] = df_tab['fields.status.name'].str.upper()
        df_tab['fields.status.name'] = np.where(df_tab['fields.status.name'].isin(to_fill_lst), 'to-fill',
                                                df_tab['fields.status.name'])
        df_tab['fields.status.name'] = np.where(df_tab['fields.status.name'].isin(in_progress_lst), 'in-progress',
                                                df_tab['fields.status.name'])
        df_tab['fields.status.name'] = np.where(df_tab['fields.status.name'].isin(to_fix_lst), 'to-fix',
                                                df_tab['fields.status.name'])
        df_tab['fields.status.name'] = np.where(df_tab['fields.status.name'].isin(complete_lst), 'complete',
                                                df_tab['fields.status.name'])
        df_tab['fields.status.name'] = np.where(df_tab['fields.status.name'].isin(ignore_lst), '-',
                                                df_tab['fields.status.name'])
        return df_tab
    
    def get_bugs(self, query, platform):
        issues = self.get_issues_by_query(query, fields = "key,status,summary,project,priority")
        df_bugs = pd.json_normalize(issues)

        df_bugs = df_bugs[['key', 'fields.status.name',  'fields.summary', 'fields.project.key', 'fields.priority.name']]
        df_bugs.rename(columns={'fields.status.name': 'status',  'fields.summary': 'name', 'fields.project.key': 'project', 'fields.priority.name':'priority' }, inplace=True)
        df_bugs['platform'] = platform
        df_bugs['name'] = '' # временно
        df_team_bugs = df_bugs
        df_team_bugs['number_of_bugs'] = 1
        df_team_bugs = df_team_bugs.groupby(['project', 'platform', 'priority']).sum()
        df_team_bugs = df_team_bugs.reset_index()
        return df_bugs, df_team_bugs
