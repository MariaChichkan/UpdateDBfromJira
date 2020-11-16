import sys
sys.path.insert(0, "/home/Shared")
sys.path.insert(0, "/home/DB_model")

from sqlalchemy.inspection import inspect
import pandas as pd
import datetime
import numpy as np
# import sys
import uuid
import pymysql

from mysql_orm import  FeatureDescriptionRow, RequestRelease, RelationRelFeature, \
    RelationSwellFeature, ReleaseRow,  DOR,  DORInformation,   ReleaseDates, Base, ApplicationQuantity, \
    ApplicationStatus

import utils

pymysql.install_as_MySQLdb()


class DBConnector:
    def __init__(self,  logger, session):
        self.logger = logger
        self.session = session
        self.engine = session.get_bind()


    def insert_db_data(self, tableclass, df_tab, update=False):
        tabname = tableclass.__tablename__
        if df_tab.shape[0] == 0:
            self.logger.info(f'__DBConnector__Info: no new rows to insert to table {tabname}')
            return
        try:
            df_db_tab = pd.read_sql(tabname, con=self.engine)
            df_db_tab.columns = [x + '_db' for x in df_db_tab.columns]
            primary_keys = [key.name for key in inspect(tableclass).primary_key if key.autoincrement != True]
            primary_keys_auto = [key.name for key in inspect(tableclass).primary_key if key.autoincrement == True]
            primary_keys_db = [x + '_db' for x in primary_keys]
            primary_keys_auto_db = [x + '_db' for x in primary_keys_auto]
            columns = [key.name for key in inspect(tableclass).columns if key.autoincrement != True]

            df_tab_to_update = pd.merge(df_tab, df_db_tab, left_on=primary_keys, right_on=primary_keys_db, how="inner")
            df_tab_to_update = df_tab_to_update[columns + primary_keys_auto_db]
            for key in primary_keys_auto_db:
                df_tab_to_update.rename(columns={key: key.rsplit('_db')[0]}, inplace=True)
            df_tab_to_insert = pd.merge(df_tab, df_db_tab, left_on=primary_keys, right_on=primary_keys_db, how="left")
            df_tab_to_insert = df_tab_to_insert[pd.isna(df_tab_to_insert[primary_keys_db[0]]) == True]
            df_tab_to_insert = df_tab_to_insert[columns]

            if df_tab_to_update.shape[0] > 0 and update:
                self.session.bulk_update_mappings(tableclass, df_tab_to_update.to_dict(orient='records'))
                self.session.commit()
                self.logger.info(f'__DBConnector__Info: updated {df_tab_to_update.shape[0]} rows in {tabname}')
            elif    df_tab_to_update.shape[0] == 0 and update:
                self.logger.info(f'__DBConnector__Info: no new rows to update {tabname}')
            if df_tab_to_insert.shape[0] > 0:
                df_tab_to_insert.to_sql(tabname, self.engine, if_exists='append', index=False)
                self.logger.info(f'__DBConnector__Info: inserted {df_tab_to_insert.shape[0]} rows to {tabname}')
            elif  df_tab_to_insert.shape[0] == 0:
                self.logger.info(f'__DBConnector__Info: no new rows to insert to {tabname}')

        except Exception as ex:
            print(ex)
            self.logger.error(f'__DBConnector__Error: {str(ex)} while inserting data to {tabname}')
            self.session.rollback()
            raise
            # sys.exit(-1)



    def get_db_data(self, tabname, query_params={}):
        params_str = ""
        params_lst = []
        df_tab = pd.DataFrame()
        for key, value in query_params.items():
            if isinstance(value, list):
                value = [f'"{str(i)}"' if isinstance(i, str) else str(i) for i in value]
                value_str = ','.join(value)
            else:
                value_str = f'"{str(value)}"' if isinstance(value, str) else str(value)
            params_lst.append(f'{key} IN ({value_str})')
        params_str = ' AND '.join(params_lst)
        if params_str:
            params_str = f' WHERE {params_str}'

        sql_query = f'SELECT * FROM `{tabname}`{params_str}'
        try:
            df_tab = pd.read_sql(sql_query, con=self.engine)
            return df_tab
        except Exception as ex:
            self.logger.error(f'__DBConnector__Error {str(ex)} while getting data from {tabname}')
            raise
            # sys.exit(-1)


class UpdateReleaseDB(DBConnector):
    def __init__(self,  logger, session):
        super().__init__(logger, session)


    @staticmethod
    def generate_id(id):
        id = str(uuid.uuid4())
        return id



    def delete_old_applications(self, project_data):
        releases = list(set(project_data['key_releases'].to_list()))
        db_data = self.get_db_data('relation_release_feature', query_params={'release_key': releases})
        db_data = pd.merge(db_data, project_data, left_on='story_key', right_on='key_stories', how="left")
        # этих stories больше нет в джире в привязке ни к каким актуальным релизам, нужно удалить эти инициативы
        old_application_lst = db_data[pd.isna(db_data['key_stories']) == True]['application_id'].to_list()
        try:
            deleted_req_apps = self.session.query(RequestRelease).filter(
                RequestRelease.application_id.in_(old_application_lst)).delete(synchronize_session='fetch')
            deleted_rel_apps = self.session.query(RelationRelFeature).filter(
                RelationRelFeature.application_id.in_(old_application_lst)).delete(synchronize_session='fetch')
            deleted_dor_apps = self.session.query(DOR).filter(DOR.application_id.in_(old_application_lst)).delete(
                synchronize_session='fetch')
            self.session.commit()
            if deleted_req_apps > 0:
                self.logger.info(
                    f'__UpdateReleaseDB__Info Deleted {deleted_req_apps} old applications from table requests_to_release')
            if deleted_rel_apps > 0:
                self.logger.info(
                    f'__UpdateReleaseDB__Info Deleted {deleted_rel_apps} old applications from table relation_release_feature')
            if deleted_dor_apps > 0:
                self.logger.info(
                    f'__UpdateReleaseDB__Info Deleted {deleted_dor_apps} old dors from table dor')
        except Exception as ex:
            self.session.rollback()
            self.logger.error(f'__UpdateReleaseDB__Error {str(ex)} while deleting old applicatons')
            raise
            # sys.exit(-1)


    def delete_old_applications_ufs(self, project_data):
        releases = list(set(project_data['key_epics'].to_list()))
        db_data = self.get_db_data('requests_to_release', query_params={'release_key': releases})
        db_data = pd.merge(db_data, project_data, left_on='application_key', right_on='key_applications',
                           how="left")
        old_application_lst = db_data[pd.isna(db_data['key_applications']) == True]['application_id'].to_list()
        try:
            deleted_req_apps = self.session.query(RequestRelease).filter(
                RequestRelease.application_id.in_(old_application_lst)).delete(synchronize_session='fetch')
            deleted_swell_apps = self.session.query(RelationSwellFeature).filter(
                RelationSwellFeature.application_id.in_(old_application_lst)).delete(synchronize_session='fetch')
            deleted_dor_apps = self.session.query(DOR).filter(DOR.application_id.in_(old_application_lst)).delete(
                synchronize_session='fetch')
            self.session.commit()
            if deleted_req_apps > 0:
                self.logger.info(
                    f'__UpdateReleaseDB__Info Deleted {deleted_req_apps} old applications from table requests_to_release')
            if deleted_swell_apps > 0:
                self.logger.info(
                    f'__UpdateReleaseDB__Info Deleted {deleted_swell_apps} old applications from table relation_swell_feature')
            if deleted_dor_apps > 0:
                self.logger.info(
                    f'__UpdateReleaseDB__Info Deleted {deleted_dor_apps} old dors from table dor')
        except Exception as ex:
            self.session.rollback()
            self.logger.error(f'__UpdateReleaseDB__Error {str(ex)} while deleting old applicatons')
            raise
            # sys.exit(-1)


        # Создадим feature_id для тех key_releases-key_stories-platform, для которых их еще нет
    def update_FeatureDescriptionRow(self, project_data, name_from, release_type):

        application_keys = list(set(project_data['key_applications'].to_list()))
        # Выберем по application_key данные по feature id и application id
        db_requestrelease = self.get_db_data('requests_to_release',
                                                   query_params={'application_key': application_keys})

        project_data = pd.merge(project_data, db_requestrelease,
                                left_on=['key_applications', f'fields.labels_{release_type}',
                                         f'key_{release_type}'],
                                right_on=['application_key', 'platform', 'release_key'], how="left")
        # Для application key ( инициатив) для которых не нашлось аписей в БД необходимо создать сгенерировать новые feature id
        df_features_to_create = project_data[pd.isna(project_data['application_id']) == True]
        df_features_to_create = df_features_to_create.sort_values(by='key_applications')
        df_features_to_create = df_features_to_create.drop_duplicates(subset='key_applications', keep="first")
        df_features_to_create['feature_id'] = df_features_to_create['feature_id'].apply(self.generate_id)
        df_features_for_app = df_features_to_create[['key_applications', 'feature_id']]
        df_features_for_app.rename(columns={'feature_id': 'feature_id_new'}, inplace=True)
        project_data = pd.merge(project_data, df_features_for_app, on='key_applications', how="left")
        project_data['feature_id'] = np.where(pd.isna(project_data['feature_id']) == True,
                                              project_data['feature_id_new'], project_data['feature_id'])
        # Сохраним новые feature id в БД
        df_features_to_create = df_features_to_create[
            [f'fields.summary_{name_from}', 'feature_id', 'fields.created_applications',
             'fields.updated_applications']]
        df_features_to_create.rename(
            columns={'fields.created_applications': 'created', 'fields.updated_applications': 'updated',
                     f'fields.summary_{name_from}': 'name'}, inplace=True)
        df_features_to_create['user'] = ''
        self.insert_db_data(FeatureDescriptionRow, df_features_to_create)
        return project_data

    def update_release_mp_web(self, project_data):
        application_key_lst = list(set(project_data['key_applications']))
        if len(application_key_lst) ==  0:
            return #нечего обновлять
        df_releases = self.get_db_data('requests_to_release', query_params={'application_key': application_key_lst})
        application_id_lst = list(set(df_releases['application_id'].to_list()))
        df_releases.rename(columns={'release_id': 'old_release_id', 'release_key': 'old_release_key'}, inplace=True)
        df_releases = pd.merge(project_data, df_releases, left_on='key_applications',
                               right_on='application_key', how="inner")
        df_releases_to_update = df_releases[df_releases['id_releases'] !=
                                            df_releases['old_release_id']]
        df_releases_to_update = df_releases_to_update[['application_id', 'application_key', 'feature_id', 'platform',
                                                       'id_releases', 'key_releases', 'segment', 'request_type', 'status']]
        df_releases = df_releases[['application_id', 'application_key', 'key_releases']]
        df_releases = df_releases.sort_values(by='application_id')
        df_releases = df_releases.drop_duplicates(subset='application_id', keep="first")

        df_releases_to_update = df_releases_to_update.sort_values(by='application_key')
        df_releases_to_update = df_releases_to_update.drop_duplicates(subset='application_key', keep="first")
        df_releases_to_update.rename(columns={'id_releases': 'release_id', 'key_releases': 'release_key'}, inplace=True)
        df_releases_to_update['status'] = df_releases_to_update['status'].fillna("")
        if df_releases_to_update.shape[0] > 0:
            self.insert_db_data(RequestRelease, df_releases_to_update, update=True)
            self.logger.info(f'__UpdateReleaseDB__Info Перепривязали релизы у {df_releases_to_update.shape[0]} инициатив в RequestRelease')
        # df_releases_to_update = df_releases_to_update[
        #     ['application_id', 'application_key', 'release_id', 'release_key']]

        if len(application_id_lst) == 0:
            return #нечего обновлять
        df_rel_feat = self.get_db_data('relation_release_feature',
                                       query_params={'application_id': application_id_lst})
        df_rel_feat.rename(columns={'release_key': 'old_release_key'}, inplace=True)

        df_rel_feat_to_update = pd.merge(df_releases, df_rel_feat, on='application_id', how="inner")
        df_rel_feat_to_update = df_rel_feat_to_update[df_rel_feat_to_update['key_releases']
                                                      != df_rel_feat_to_update['old_release_key']]
        df_rel_feat_to_update.rename(columns={'key_releases': 'release_key'}, inplace=True)
        df_rel_feat_to_update = df_rel_feat_to_update[['application_id', 'platform', 'feature_name', 'release_key',
                                                       'story_key', 'type', 'result', 'error_code',
                                                       'last_update']]

        if df_rel_feat_to_update.shape[0] > 0:
            df_rel_feat_to_update['last_update'] = str(datetime.datetime.now().replace(microsecond=0))
            self.logger.info(
                    f'__UpdateReleaseDB__Info Перепривязали релизы у {df_rel_feat_to_update.shape[0]} инициатив в RelationRelFeature')
            self.insert_db_data(RelationRelFeature, df_rel_feat_to_update, update=True)

    def update_release_ufs(self, project_data):
        # Получим данные которые уже есть по application_key (для проверки по ним корректности релиза)
        application_key_lst = list(set(project_data['key_applications']))
        if len(application_key_lst) == 0:
            return #нечего обновлять
        df_releases = self.get_db_data('requests_to_release',
                                             query_params={'application_key': application_key_lst})
        df_releases.rename(columns={'release_id': 'old_release_id', 'release_key': 'old_release_key'}, inplace=True)
        df_releases = pd.merge(project_data, df_releases, left_on='key_applications',
                               right_on='application_key', how="inner")

        df_releases_to_update = df_releases[df_releases['id_epics'] !=
                                            df_releases['old_release_id']]
        df_releases_to_update = df_releases_to_update[['application_id', 'application_key', 'feature_id', 'platform',
                                                       'id_epics', 'key_epics', 'segment', 'request_type', 'status']]
        df_releases_to_update = df_releases_to_update.sort_values(by='application_id')
        df_releases_to_update = df_releases_to_update.drop_duplicates(subset='application_id', keep="first")
        df_releases_to_update.rename(columns={'id_epics': 'release_id', 'key_epics': 'release_key'}, inplace=True)
        df_releases = df_releases[['application_id', 'key_epics']]
        df_releases = df_releases.sort_values(by='application_id')
        df_releases = df_releases.drop_duplicates(subset='application_id', keep="first")
        df_releases.rename(columns={'key_epics': 'release_key'}, inplace=True)
        application_id_lst = list(set(df_releases['application_id'].to_list()))

        if df_releases_to_update.shape[0] > 0:
            self.logger.info(
                    f'__UpdateReleaseDB__Info Перепривязали эпики у {df_releases_to_update.shape[0]} инициатив в RequestRelease')
            self.insert_db_data(RequestRelease, df_releases_to_update, update=True)

        if len(application_id_lst) == 0:
            return #нечего обновлять
        df_rel_swell = self.get_db_data('relation_swell_feature',
                                              query_params={'application_id': application_id_lst})
        df_rel_swell.rename(columns={'release_key': 'release_key_epic'}, inplace=True)
        df_rel_swell_to_update = pd.merge(df_releases, df_rel_swell, on='application_id', how="inner")

        df_rel_swell_to_update = df_rel_swell_to_update[df_rel_swell_to_update['release_key']
                                                        != df_rel_swell_to_update['swell_key']]
        df_rel_swell_to_update = df_rel_swell_to_update[
            ['application_id', 'release_key', 'platform', 'feature_name', 'release_key_epic', 'request_type', 'result',
             'error_code', 'last_update', 'channel']]
        df_rel_swell_to_update.rename(columns={'release_key': 'swell_key', 'release_key_epic': 'release_key'},
                                      inplace=True)

        if df_rel_swell_to_update.shape[0] > 0:
            df_rel_swell_to_update['last_update'] = str(datetime.datetime.now().replace(microsecond=0))
            self.insert_db_data(RelationSwellFeature, df_rel_swell_to_update, update=True)
            self.logger.info(
                f'__UpdateReleaseDB__Info Перепривязали эпики у {df_rel_swell_to_update.shape[0]} инициатив в RelationSwellFeature')

    def update_application_keys(self, project_data, project_type):
        # Для тех заявок в релиз, которые создавались через портал нужно проапдейтить/заполнить application_key по application_id
        # Для этого сначала найдем уже созданные application_id по релизу, платорме и сторе
        key_releases_lst = list(set(project_data['key_releases'].to_list()))
        if project_type == 'mp_web':
            df_db_rel_feat = self.get_db_data('relation_release_feature',
                                                    query_params={'release_key': key_releases_lst})
            df_releases_to_update = pd.merge(project_data, df_db_rel_feat,
                                             left_on=['key_releases', 'fields.labels_applications', 'key_stories'],
                                             right_on=['release_key', 'platform', 'story_key'], how="inner")
            df_releases_to_update = df_releases_to_update[['application_id', 'key_applications']]

        elif project_type == 'ufs':
            project_data['key_stories']=''
            df_db_rel_feat = self.get_db_data('relation_swell_feature',
                                                    query_params={'swell_key': key_releases_lst})
            df_releases_to_update = pd.merge(project_data, df_db_rel_feat,
                                             left_on=['key_epics', 'fields.labels_applications'],
                                             right_on=['release_key', 'platform'], how="inner")
            df_releases_to_update = df_releases_to_update[['application_id', 'key_applications']]

        application_id_lst = list(set(df_releases_to_update['application_id'].to_list()))
        if len(application_id_lst) == 0:
            return #нечего обновлять
        # Получим данные которые есть по уже созданным application_id (для обновения по ним application_key)
        df_releases = self.get_db_data('requests_to_release', query_params={'application_id': application_id_lst})
        df_releases_to_update = pd.merge(df_releases, df_releases_to_update, on='application_id', how="inner")

        df_releases_to_update = df_releases_to_update.sort_values(by='application_id')
        df_releases_to_update = df_releases_to_update.drop_duplicates(subset='application_id', keep="first")
        # Оставим те строки где либо application_key не заполнен либо он не совпадает в jira (перепривязали/неверно привязали)
        df_releases_to_update = df_releases_to_update[(pd.isna(df_releases_to_update['application_key']) == True) | (
                    df_releases_to_update['application_key'] == "") | (df_releases_to_update['application_key'] !=
                                                                       df_releases_to_update['key_applications'])]
        df_releases_to_update['application_key'] = df_releases_to_update['key_applications']
        df_releases_to_update = df_releases_to_update[
            ['application_id', 'application_key', 'feature_id', 'platform', 'release_id', 'release_key', 'segment',
             'request_type', 'status']]
        if df_releases_to_update.shape[0]>0:
            self.insert_db_data(RequestRelease, df_releases_to_update, update=True)

    def update_RequestRelease(self, project_data, release_type):
        df_apps_to_create = project_data[pd.isna(project_data['application_id']) == True]
        df_apps_to_create = df_apps_to_create.sort_values(by='key_applications')
        df_apps_to_create = df_apps_to_create.drop_duplicates(subset='key_applications', keep="first")
        df_apps_to_create = df_apps_to_create[
            ['application_id', 'key_applications', 'feature_id', f'fields.labels_{release_type}', f'id_{release_type}',
             f'key_{release_type}', 'fields.status.name_applications']]
        df_apps_to_create.rename(
            columns={'key_applications': 'application_key', f'fields.labels_{release_type}': 'platform',
                     f'id_{release_type}': 'release_id', f'key_{release_type}': 'release_key', 'fields.status.name_applications': 'jira_status' }, inplace=True)
        df_apps_to_create['segment'] = 'no-segment'
        df_apps_to_create['request_type'] = 'no-type'
        df_apps_to_create['application_id'] = df_apps_to_create['application_id'].apply(self.generate_id)
        platforms = list(set(project_data[f'fields.labels_{release_type}'].to_list()))
        platforms = [x for x in platforms if str(x) != 'nan']
        df_app_status = self.get_db_data('application_status',  query_params={'platform': platforms})
        df_apps_to_create['jira_status'] = df_apps_to_create['jira_status'].str.upper()
        df_app_status['jira_status'] = df_app_status['jira_status'].str.upper()
        df_apps_to_create = pd.merge(df_apps_to_create, df_app_status, on=['jira_status', 'platform'], how = "left")
        df_apps_to_create = df_apps_to_create[['application_id', 'application_key', 'platform', 'feature_id', 'release_id', 'release_key', 'status', 'segment', 'request_type']]
        self.insert_db_data(RequestRelease, df_apps_to_create)
        df_apps_to_create = df_apps_to_create[['application_id', 'application_key']]
        df_apps_to_create.rename(
            columns={'application_id': 'application_id_new', 'application_key': 'key_applications'}, inplace=True)
        project_data = pd.merge(project_data, df_apps_to_create, on='key_applications', how="left")
        project_data['application_id'] = np.where(pd.isna(project_data['application_id']) == True,
                                                  project_data['application_id_new'], project_data['application_id'])
        return project_data


    def update_application_status(self, project_data):
        try:
            mew_app_statuses = project_data[['fields.status.name_applications', 'application_id', 'platform']]
            mew_app_statuses = mew_app_statuses.drop_duplicates(subset='application_id', keep="first")
            mew_app_statuses.rename(columns={'fields.status.name_applications': 'jira_status'}, inplace=True)
            mew_app_statuses['jira_status'] = mew_app_statuses['jira_status'].str.upper()

            platform_list = list(set(mew_app_statuses['platform'].to_list()))
            platform_list = [x for x in platform_list if str(x) != 'nan']
            application_status = pd.read_sql(
                self.session.query(ApplicationStatus).filter(ApplicationStatus.platform.in_(platform_list)).statement,
                self.engine)
            application_status['jira_status'] = application_status['jira_status'].str.upper()

            mew_app_statuses = pd.merge(mew_app_statuses, application_status, on=['jira_status', 'platform'],
                                        how="left")
            mew_app_statuses = mew_app_statuses[['application_id', 'status']]
            mew_app_statuses['status'] = mew_app_statuses['status'].fillna("")

            for key, val in mew_app_statuses.iterrows():
                self.session.query(RequestRelease).filter(RequestRelease.application_id == val[0]).update({"status": val[1]})
            self.logger.info(f'__UpdateReleaseDB__Info Обновили статусы заявок в релиз для платформ {platform_list}')
            self.session.commit()
        except Exception as ex:
            self.logger.error(f'__UpdateReleaseDB__Error При обновлении статусов заявок в релиз ошибка: {ex}')
            self.session.rollback()
            raise


    def update_RelationRelFeature(self, project_data):
        df_new_relat = pd.DataFrame()
        df_new_relat = project_data[
            ['application_id', 'fields.labels_applications', 'fields.summary_stories', 'key_stories', 'key_releases']]
        df_new_relat.rename(
            columns={'fields.labels_applications': 'platform', 'fields.summary_stories': 'feature_name', 'key_stories': 'story_key',
                     'key_releases': 'release_key'}, inplace=True)
        df_new_relat['type'] = ""
        df_new_relat['result'] = ""
        df_new_relat['error_code'] = ""
        df_new_relat['last_update'] = str(datetime.datetime.now().replace(microsecond=0))
        df_new_relat = df_new_relat.sort_values(by='application_id')
        df_new_relat = df_new_relat.drop_duplicates(subset='application_id', keep="first")
        self.insert_db_data(RelationRelFeature, df_new_relat)

    def update_DOR(self, project_data, project_type, DOR_type):  # subtasks, UXUI
        DOR_jira_key = 'key_' + DOR_type
        DOR_jira_lables = 'fields.labels_' + DOR_type
        DOR_jira_lables_1 = 'fields.labels_' + DOR_type + '_1'
        DOR_jira_status = 'fields.status.name_' + DOR_type
        DOR_content = 'content_' + DOR_type
        DOR_assignee_key = 'fields.assignee.key_' + DOR_type
        DOR_assignee_email = 'fields.assignee.emailAddress_' + DOR_type
        DOR_assignee_name = 'fields.assignee.displayName_' + DOR_type
        DOR_last_update = 'fields.updated_' + DOR_type
        DOR_description_wo_url = 'fields.description_wo_url_' + DOR_type
        DOR_urls = 'urls_' + DOR_type
        DOR_type_id = 'dor_type_id_'+ DOR_type

        df_dor_information = self.get_db_data('dor_info')
        project_data = pd.merge(project_data, df_dor_information, left_on= DOR_type_id, right_on='dor_type_id',
                                how="inner")
        project_data['content'] = ''
        project_data['content'] = np.where(project_data['dor_type'] == 'string' , project_data[DOR_description_wo_url],
                                           project_data['content'])
        project_data['content'] = np.where( project_data['dor_type'] == 'readyornot', project_data[DOR_description_wo_url],
                                           project_data['content'])
        project_data['content'] = np.where(project_data['dor_type'] == 'link', project_data[DOR_urls],
                                           project_data['content'])
        project_data['content'] = np.where(project_data['dor_type'] == 'jira-link', project_data[DOR_jira_key],
                                           project_data['content'])
        project_data[DOR_assignee_key] = np.where(pd.isna(project_data[DOR_assignee_key]) == True, '',
                                                  project_data[DOR_assignee_key])
        project_data[DOR_assignee_email] = np.where(pd.isna(project_data[DOR_assignee_email]) == True, '',
                                                    project_data[DOR_assignee_email])
        project_data[DOR_assignee_name] = np.where(pd.isna(project_data[DOR_assignee_name]) == True, '',
                                                   project_data[DOR_assignee_name])
        db_new_dor = pd.DataFrame()
        db_new_dor = project_data[
            ['application_id', DOR_jira_key, 'dor_type_id', 'content', DOR_jira_status, DOR_assignee_key,
             DOR_assignee_email, DOR_assignee_name, DOR_last_update]]
        db_new_dor.rename(
            columns={DOR_jira_key: 'subtask_key', DOR_jira_status: 'status', DOR_assignee_key: 'assignee_key',
                     DOR_assignee_email: 'assignee_email', DOR_assignee_name: 'assignee_name',
                     DOR_last_update: 'last_update'}, inplace=True)
        db_new_dor['user'] = ''
        db_new_dor = db_new_dor.sort_values(by=['application_id', 'subtask_key'])
        db_new_dor = db_new_dor.drop_duplicates(subset=['application_id', 'subtask_key'], keep="first")
        df_db_dor = self.get_db_data('dor')
        df_db_dor['exist_in_db'] = 'X'
        db_new_dor = pd.merge(db_new_dor, df_db_dor,
                        on=['application_id', 'subtask_key', 'dor_type_id', 'content', 'status', 'user', 'assignee_key',
                            'assignee_name', 'assignee_email', 'last_update'], how="left")
        db_new_dor = db_new_dor[db_new_dor['exist_in_db'] != 'X']
        db_new_dor = db_new_dor[['application_id', 'subtask_key', 'dor_type_id', 'content', 'status', 'user', 'assignee_key',
                     'assignee_name', 'assignee_email', 'last_update']]
        self.insert_db_data(DOR, db_new_dor, update=True)

    def update_RelationSwellFeature(self, project_data):
        df_new_swell = pd.DataFrame()
        df_new_swell = project_data[['application_id', 'fields.labels_epics', 'fields.labels_applications', 'channel_applications', 'key_epics', 'release_key']]
        df_new_swell.rename(columns={'fields.labels_applications': 'request_type', 'fields.labels_epics': 'platform', 'channel_applications': 'channel',
                                     'key_epics': 'swell_key'}, inplace=True)
        df_new_swell['feature_name'] = ''
        df_new_swell['release_key'] = ''
        df_new_swell['result'] = ''
        df_new_swell['error_code'] = ''
        df_new_swell['last_update'] = ''
        df_new_swell = df_new_swell.sort_values(by='application_id')
        df_new_swell = df_new_swell.drop_duplicates(subset='application_id', keep="first")
        self.insert_db_data(RelationSwellFeature, df_new_swell)


    def update_mp_web_db(self, project_data):
        # Для тех заявок в релиз, которые создавались через портал нужно проапдейтить/заполнить application_key по application_id
        # Для этого сначала найдем уже созданные application_id по релизу, платорме и сторе/ эпику и платорме
        self.update_application_keys(project_data, 'mp_web')
        self.update_release_mp_web(project_data)
        # удалим стори которые отвязали от релиза вместе с их инициативами и дорами
        self.delete_old_applications(project_data)
        # Сгенерируем feature_id для тех key_releases-key_stories-platform, для которых их еще нет
        project_data = self.update_FeatureDescriptionRow(project_data, 'stories', 'releases')
        # Сгенерируем application_id для тех  application_key-feature_id-platform-release_id для которых их еще нет
        project_data = self.update_RequestRelease(project_data, 'releases')
        self.update_application_status(project_data)
        # Сохраним связь новых application_id (если они есть) со stories
        self.update_RelationRelFeature(project_data)
        self.update_DOR(project_data, 'mp_web', 'subtasks')
        self.update_DOR(project_data, 'mp_web', 'UXUI')
        release_lst = list(set(project_data['key_releases'].to_list()))
        utils.update_teams_and_apps(self.session,release_lst)


    def update_ufs_db(self, project_data):
        # Для тех заявок в релиз, которые создавались через портал нужно проапдейтить/заполнить application_key по application_id
        # Для этого сначала найдем уже созданные application_id по релизу, платорме и сторе/ эпику и платорме
        self.update_application_keys(project_data, 'ufs')
        self.update_release_ufs(project_data)
        # удалим инициативы ефс которые отвязали от релиза вместе с дорами
        self.delete_old_applications_ufs(project_data)
        # Сгенерируем feature_id для тех key_epics-key_applications, для которых их еще нет
        project_data = self.update_FeatureDescriptionRow(project_data, 'applications', 'epics')
        project_data = self.update_RequestRelease(project_data, 'epics')
        self.update_application_status(project_data)
        self.update_RelationSwellFeature(project_data)
        self.update_DOR(project_data, 'ufs', 'subtasks')  # subtasks, UXUI
        self.update_DOR(project_data, 'ufs', 'UXUI')
        release_lst = list(set(project_data['key_epics'].to_list()))
        utils.update_teams_and_apps(self.session, release_lst)


