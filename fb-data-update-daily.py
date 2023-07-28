import requests
import json
import pandas as pd
import datetime
import re
import sys
import time
import configparser
import traceback
import numpy as np
import base64
from datetime import timezone
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage
import os
from random import sample




####################################### START LOCAL TESTING ONLY

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cfg/nmas-369423-feec42c707c9.json"

####################################### END LOCAL TESTING
#notas generales: Especificar mejor las variables, todas las funciones que traigan información, homologar con un get,
#  cambiar todos los nombres de las funciones config para que se note que son configuraciones de big query.
# cambiar versión de api a la 16
# variables en español se quedan como deuda técnica  


"""
Define utility functions
"""

def post_slack_updates(slack_message) -> None:
    
    # Build the headers for the slack
    headers = {'Content-type': 'application/json'}
    
    # Add the message to the payload
    payload = {'text': slack_message}
    
    # Dump the payload into a JSON
    payload = json.dumps(payload)
    
    # Post the message
    requests.post(slack_webhook, data = payload, headers = headers)

    return None

def read_config_files(bucket_name) -> tuple[str, str, str, str, str, str, str, str, str, str, str, str]:
    
    # Instantiate the storage client to read files from the bucket
    storage_client = storage.Client()

    # Instantiate the configuration parser
    config_parser = configparser.RawConfigParser()
    
    # Instantiate a storage client using provided bucket name
    bucket = storage_client.get_bucket(bucket_name)
    
    # Read the blob from the bucket, download it as bytes and decode it
    blob = bucket.get_blob('fb-cfg/facebook-api.cfg')
    cfg = blob.download_as_bytes().decode()
    
    # Parse the configuration decoded as string
    config_parser.read_string(cfg)
    
    # Read the token from the configuration file and section
    token = config_parser.get('facebook-api-config', 'token')
    
    # Read the ig_user_id from the configuration file and section
    id_fb_user = config_parser.get('facebook-api-config', 'id_fb_user')
    id_fb_user = int(id_fb_user)

    # Read table names from the configuration file and section
    fb_page_insights_table = config_parser.get('facebook-api-config','fb_page_insights_table')
    fb_post_insights_table = config_parser.get('facebook-api-config', 'fb_post_insights_table')

    fb_page_info_table=config_parser.get('facebook-api-config','fb_page_info_table')
    fb_post_info_table=config_parser.get('facebook-api-config','fb_post_info_table')

    fb_albums_info_table=config_parser.get('facebook-api-config','fb_albums_info_table')
    fb_photo_albums_info_table=config_parser.get('facebook-api-config','fb_photo_albums_info_table')
    fb_video_lists_info_table=config_parser.get('facebook-api-config','fb_video_lists_info_table')
    fb_video_info_table=config_parser.get('facebook-api-config','fb_video_info_table')
    fb_live_video_info_table=config_parser.get('facebook-api-config','fb_live_video_info_table')

    slack_webhook = config_parser.get('facebook-api-config', 'slack_webhook')
    
    # Return the token and id_fb_user
    return token, id_fb_user,fb_page_insights_table,fb_post_insights_table,fb_page_info_table,fb_post_info_table,fb_albums_info_table,fb_photo_albums_info_table,fb_video_lists_info_table,fb_video_info_table,fb_live_video_info_table,slack_webhook

def frame_validate(keys, frame) -> pd.DataFrame:
    for key in keys:
        #if key doesn´t exist in dataframe, add colummn with his name and value null  
        if key not in frame.columns:
            frame.insert(0,key,np.nan)
    #dataframe re order        
    frame=frame[keys]
    return frame

def upload_bigquery_data(frame, tabla, fb_table, job_config) -> None:
    
    # Instantiate a BigQuery Client
    client = bigquery.Client()
    
    # Save job config locally
    bq_job_config = job_config

    # Try to upload data
    try:
        # Table upload with job_config to bigquery 
        job = client.load_table_from_dataframe(frame, fb_table, job_config = bq_job_config)
    
    # If upload fails, catch exception
    except Exception as e:
        slack_update = f'ERROR: fb_data_update job failed on upload_bigquery_data step in table: {tabla} with exception {e}'
        print(slack_update)
        post_slack_updates(slack_update)
        raise

    # Success
    print(f"{tabla} data uploaded successfully.")

    # End function excution
    return

def get_info_from_url(url, tabla) -> dict:

    # Try to send the request to the API and Return JSON object of the response 
    try:
        info = requests.get(url).json()

    # If request fails, catch exception
    except Exception as e:
        print(e)
        slack_update = f'ERROR: fb_data_update job failed on get_info_from_url in step:{tabla}, job failed with exception: {e}'
        #slack_update = 'ERROR: fb_data_update job failed on delete_fb_tables step with exception {}'.format(e)
        print(slack_update)
        post_slack_updates(slack_update)
        raise
    
    # Return result
    return info


def page_insights_to_dataframe(info, id_fb_user) -> pd.DataFrame:
    
    # Instantiate an empty list to store column names
    columns = []
    
    # Instantiate an empty list to store column values
    values = []

    # Iterate over each element in the dictionary
    for element in range(len(info['data'])):
        
        # Append the name of the element to the columns list
        columns.append(info['data'][element]['name'])

        # Append the value of the element to the column values list
        values.append(info['data'][element]['values'][0]['value'])
    

    # Zip both lists to a datafra,e
    frame = pd.DataFrame(zip(columns, values))
    
    # Transpose the dataframe to emulate a table
    frame = frame.transpose()
    
    # Select headers of DataFrame 
    headers = frame.iloc[0]
    times_page = info['data'][0]['values'][0]['end_time']

    # Clean timestamp and set end_time to a day before end_time
    #times_page = times_page[:-5]
    #temp_timestamp = datetime.strptime(times_page, "%Y-%m-%dT%H:%M:%S")
    #times_page = str(datetime.strptime(str(temp_timestamp), "%Y-%m-%d %H:%M:%S")) + '.00000'
    
    # Set values and headers in dataframe  
    frame = pd.DataFrame(frame.values[1:], columns = headers)
    
    # Sdd id_fb_user, end_time and process_timestamp to the dataframe
    frame.insert(0, "id_fb_user", id_fb_user, allow_duplicates = False)
    frame.insert(1, "end_time", times_page, allow_duplicates = False)
    frame.insert(2, "process_timestamp", datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'), allow_duplicates = False)
    
    # Set id_fb_user as INT and end_time as timestamp
    frame['id_fb_user'] = frame['id_fb_user'].astype(int)
    frame['end_time'] = pd.to_datetime(frame['end_time'], format = '%Y-%m-%dT%H:%M:%S+0000') - timedelta(days = 1)

    return frame

"""
User info
"""

def bigquery_config_user_info(dframe, tabla, fb_page_info_table) -> None:
    
    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
            
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "page_fans_locale" column uses pandas dtype "object", so its
            # data type is ambiguous.          
            bigquery.SchemaField(name ='id_fb_user', field_type = 'INTEGER'),
            bigquery.SchemaField(name ='end_time', field_type = 'TIMESTAMP'),
            bigquery.SchemaField(name ='process_timestamp', field_type = 'TIMESTAMP'),
            bigquery.SchemaField(name ='about', field_type = 'STRING'),
            bigquery.SchemaField(name ='can_post', field_type = 'BOOLEAN'),
            bigquery.SchemaField(name ='category', field_type = 'STRING'),
            bigquery.SchemaField(name ='category_list', field_type = 'STRING'),
            bigquery.SchemaField(name ='checkins', field_type = 'INTEGER'),
            bigquery.SchemaField(name ='copyright_whitelisted_ig_partners', field_type = 'STRING'),
            bigquery.SchemaField(name ='country_page_likes', field_type = 'INTEGER'),
            bigquery.SchemaField(name ='description',field_type ='STRING'),
            bigquery.SchemaField(name ='description_html',field_type ='STRING'),
            bigquery.SchemaField(name ='display_subtext',field_type ='STRING'),
            bigquery.SchemaField(name ='displayed_message_response_time',field_type ='STRING'),
            bigquery.SchemaField(name ='fan_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='followers_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='founded',field_type ='INTEGER'),
            bigquery.SchemaField(name ='global_brand_page_name',field_type ='STRING'),
            bigquery.SchemaField(name ='has_added_app',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='has_transitioned_to_new_page_experience',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='has_whatsapp_business_number',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='has_whatsapp_number',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='instant_articles_review_status',field_type ='STRING'),
            bigquery.SchemaField(name ='is_always_open',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_chain',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_community_page',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_eligible_for_branded_content',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_messenger_bot_get_started_enabled',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_messenger_platform_bot',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_owned',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_permanently_closed',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_published',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_unclaimed',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_webhooks_subscribed',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='leadgen_tos_accepted',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='link',field_type ='STRING'),
            bigquery.SchemaField(name ='merchant_review_status',field_type ='STRING'),
            bigquery.SchemaField(name ='messenger_ads_default_icebreakers',field_type ='STRING'),
            bigquery.SchemaField(name ='messenger_ads_quick_replies_type',field_type ='STRING'),
            bigquery.SchemaField(name ='mission',field_type ='STRING'),
            bigquery.SchemaField(name ='name',field_type ='STRING'),
            bigquery.SchemaField(name ='name_with_location_descriptor',field_type ='STRING'),
            bigquery.SchemaField(name ='new_like_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='offer_eligible',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='page_token',field_type ='STRING'),
            bigquery.SchemaField(name ='promotion_eligible',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='supports_donate_button_in_live_video',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='supports_instant_articles',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='talking_about_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='temporary_status',field_type ='STRING'),
            bigquery.SchemaField(name ='unread_message_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='unread_notif_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='unseen_message_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='username',field_type ='STRING'),
            bigquery.SchemaField(name ='verification_status',field_type ='STRING'),
            bigquery.SchemaField(name ='website',field_type ='STRING'),
            bigquery.SchemaField(name ='were_here_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='business_id',field_type ='INTEGER'),
            bigquery.SchemaField(name ='business_name',field_type ='STRING'),
            bigquery.SchemaField(name ='connected_instagram_account_id',field_type ='INTEGER'),
            bigquery.SchemaField(name ='connected_page_backed_instagram_account_id',field_type ='INTEGER'),
            bigquery.SchemaField(name ='cover_cover_id',field_type ='INTEGER'),
            bigquery.SchemaField(name ='cover_offset_x',field_type ='INTEGER'),
            bigquery.SchemaField(name ='cover_offset_y',field_type ='INTEGER'),
            bigquery.SchemaField(name ='cover_source',field_type ='STRING'),
            bigquery.SchemaField(name ='cover_id',field_type ='INTEGER'),
            bigquery.SchemaField(name ='engagement_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='engagement_social_sentence',field_type ='STRING'),
            bigquery.SchemaField(name ='instagram_business_account_id',field_type ='INTEGER'),
            bigquery.SchemaField(name ='messaging_feature_status_hop_v2',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='start_info_type',field_type ='STRING'),
            bigquery.SchemaField(name ='voip_info_has_permission',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='voip_info_has_mobile_app',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='voip_info_is_pushable',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='voip_info_is_callable',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='voip_info_is_callable_webrtc',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='voip_info_reason_code',field_type ='INTEGER'),
            bigquery.SchemaField(name ='voip_info_reason_description',field_type ='STRING')

        ]
    )
    
    # Upload data to BigQuery
    upload_bigquery_data(dframe, tabla, fb_page_info_table, job_config)
    #print(f"{tabla} data uploaded successfully. CHECK")

    return 0

def get_user_info(id_fb_user, since, tabla, fb_page_info_table) -> None:

    #Empty list for user data 
    userdata = []

    # URL for the endpoint of user info
    user_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/?fields=about,business,can_post,category,category_list,checkins,connected_instagram_account,connected_page_backed_instagram_account,copyright_whitelisted_ig_partners,country_page_likes,cover,description,description_html,display_subtext,displayed_message_response_time,engagement,fan_count,followers_count,founded,global_brand_page_name,has_added_app,has_transitioned_to_new_page_experience,has_whatsapp_business_number,has_whatsapp_number,id,instagram_business_account,instant_articles_review_status,is_always_open,is_chain,is_community_page,is_eligible_for_branded_content,is_messenger_bot_get_started_enabled,is_messenger_platform_bot,is_owned,is_permanently_closed,is_published,is_unclaimed,is_webhooks_subscribed,leadgen_tos_accepted,link,merchant_review_status,messaging_feature_status,messenger_ads_default_icebreakers,messenger_ads_quick_replies_type,mission,name,name_with_location_descriptor,new_like_count,offer_eligible,page_token,promotion_eligible,supports_donate_button_in_live_video,supports_instant_articles,talking_about_count,temporary_status,unread_message_count,unread_notif_count,unseen_message_count,username,verification_status,voip_info,website,were_here_count&access_token={token}'
    
    # Get user info from the endpoint
    info_url = get_info_from_url(user_url, 'user_info')
       
    # Add the current values to the empty list
    userdata.append(info_url)
    
    # If user data is not empty
    if len(userdata) > 0:

        # Column order to match database table
        column_order = ['id_fb_user','end_time','process_timestamp','about','can_post','category','category_list','checkins',
                        'copyright_whitelisted_ig_partners','country_page_likes','description','description_html',
                        'display_subtext','displayed_message_response_time','fan_count','followers_count','founded',
                        'global_brand_page_name','has_added_app','has_transitioned_to_new_page_experience',
                        'has_whatsapp_business_number','has_whatsapp_number','instant_articles_review_status',
                        'is_always_open','is_chain','is_community_page','is_eligible_for_branded_content',
                        'is_messenger_bot_get_started_enabled','is_messenger_platform_bot','is_owned','is_permanently_closed',
                        'is_published','is_unclaimed','is_webhooks_subscribed','leadgen_tos_accepted','link',
                        'merchant_review_status','messenger_ads_default_icebreakers','messenger_ads_quick_replies_type',
                        'mission','name','name_with_location_descriptor','new_like_count','offer_eligible','page_token',
                        'promotion_eligible','supports_donate_button_in_live_video','supports_instant_articles',
                        'talking_about_count','temporary_status','unread_message_count','unread_notif_count',
                        'unseen_message_count','username','verification_status','website','were_here_count','business_id',
                        'business_name','connected_instagram_account_id','connected_page_backed_instagram_account_id',
                        'cover_cover_id','cover_offset_x','cover_offset_y','cover_source','cover_id','engagement_count',
                        'engagement_social_sentence','instagram_business_account_id','messaging_feature_status_hop_v2',
                        'start_info_type','voip_info_has_permission','voip_info_has_mobile_app','voip_info_is_pushable',
                        'voip_info_is_callable','voip_info_is_callable_webrtc','voip_info_reason_code',
                        'voip_info_reason_description']
        
        # Transform json object to a dataframe
        user_info_dataframe = pd.json_normalize(userdata)

        # Add process timestamp to dataframe
        user_info_dataframe.insert(0, "process_timestamp", datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))

        # Add end_time dataframe
        user_info_dataframe.insert(0, "end_time", since)

        # Rename ig_user_column
        user_info_dataframe.rename(columns = {'id':'id_fb_user','business.id':'business_id','business.name':'business_name','connected_instagram_account.id':'connected_instagram_account_id','connected_page_backed_instagram_account.id':'connected_page_backed_instagram_account_id','cover.cover_id':'cover_cover_id','cover.offset_x':'cover_offset_x','cover.offset_y':'cover_offset_y','cover.source':'cover_source','cover.id':'cover_id','engagement.count':'engagement_count','engagement.social_sentence':'engagement_social_sentence','instagram_business_account.id':'instagram_business_account_id','messaging_feature_status.hop_v2':'messaging_feature_status_hop_v2','start_info.type':'start_info_type','voip_info.has_permission':'voip_info_has_permission','voip_info.has_mobile_app':'voip_info_has_mobile_app','voip_info.is_pushable':'voip_info_is_pushable','voip_info.is_callable':'voip_info_is_callable','voip_info.is_callable_webrtc':'voip_info_is_callable_webrtc','voip_info.reason_code':'voip_info_reason_code','voip_info.reason_description':'voip_info_reason_description'}, inplace = True)
        user_info_dataframe['id_fb_user'] = id_fb_user
        
        
        # Process columns to set as integer
        user_info_columns_int = ['id_fb_user','checkins','country_page_likes','fan_count','followers_count','founded','new_like_count','talking_about_count','unread_message_count','unread_notif_count','unseen_message_count','were_here_count','business_id','connected_instagram_account_id','connected_page_backed_instagram_account_id','cover_cover_id','cover_offset_x','cover_offset_y','cover_id','engagement_count','instagram_business_account_id','voip_info_reason_code']

        # Process columns to set as string
        user_info_columns_str = ['about','category','category_list','copyright_whitelisted_ig_partners','description','description_html','display_subtext','displayed_message_response_time','global_brand_page_name','instant_articles_review_status','link','merchant_review_status','messenger_ads_default_icebreakers','messenger_ads_quick_replies_type','mission','name','name_with_location_descriptor','page_token','temporary_status','username','verification_status','website','business_name','cover_source','engagement_social_sentence','start_info_type','voip_info_reason_description']

        #function call frame_validate to add keys wich doesn't exist 
        user_info_dataframe = frame_validate(column_order, user_info_dataframe)

        # Transform column types
        user_info_dataframe[user_info_columns_str] = user_info_dataframe[user_info_columns_str].astype(str)
        user_info_dataframe[user_info_columns_int] = user_info_dataframe[user_info_columns_int].astype(int,errors='ignore')
        user_info_dataframe['end_time'] = pd.to_datetime(user_info_dataframe['end_time'], format = '%Y-%m-%d')
        user_info_dataframe['process_timestamp'] = pd.to_datetime(user_info_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')

        

        # Upload dataframe to BigQuery
        bigquery_config_user_info(user_info_dataframe[column_order], 'user_info', fb_page_info_table)

    
        #print(f"SUCCESS: user_info uploaded to {tabla} succesfully.")
    return

"""
Page insights
"""

def bigquery_config_page_insights(frame, tabla, fb_page_insights_table):

    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
        #Specify the type of columns whose type cannot be auto-detected. For
        #example the "page_fans_locale" column uses pandas dtype "object", so its
        #data type is ambiguous.
        bigquery.SchemaField(name = 'id_fb_user', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'end_time', field_type = 'TIMESTAMP'),
        bigquery.SchemaField(name = 'process_timestamp', field_type = 'TIMESTAMP'),
        bigquery.SchemaField(name = 'page_total_actions', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_cta_clicks_logged_in_total', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_cta_clicks_logged_in_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_call_phone_clicks_logged_in_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_get_directions_clicks_logged_in_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_website_clicks_logged_in_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_website_clicks_by_site_logged_in_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_engaged_users', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_post_engagements', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_consumptions', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_consumptions_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_consumptions_by_consumption_type', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_places_checkin_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_places_checkin_total_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_places_checkin_mobile', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_places_checkin_mobile_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_negative_feedback', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_negative_feedback_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_negative_feedback_by_type', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_negative_feedback_by_type_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_positive_feedback_by_type', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_positive_feedback_by_type_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fans_online', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fans_online_per_day', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_fan_adds_by_paid_non_paid_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_impressions', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_paid_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_organic_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_viral', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_viral_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_nonviral', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_nonviral_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_impressions_by_story_type_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_impressions_frequency_distribution', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_impressions_viral_frequency_distribution', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_posts_impressions', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_paid_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_organic_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_served_impressions_organic_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_viral', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_viral_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_nonviral', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_nonviral_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_posts_impressions_frequency_distribution', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_actions_post_reactions_like_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_actions_post_reactions_love_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_actions_post_reactions_wow_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_actions_post_reactions_haha_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_actions_post_reactions_sorry_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_actions_post_reactions_anger_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_actions_post_reactions_total', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fans', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_fans_locale', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fans_city', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fans_country', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fans_gender_age', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fan_adds_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_fans_by_like_source', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fans_by_like_source_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_fan_removes', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_fan_removes_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_fans_by_unlike_source_unique', field_type = 'STRING'),     
        bigquery.SchemaField(name = 'page_video_views', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_by_paid_non_paid', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_video_views_autoplayed', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_click_to_play', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_repeat_views', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_complete_views_30s', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_complete_views_30s_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_complete_views_30s_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_complete_views_30s_autoplayed', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_complete_views_30s_click_to_play', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_complete_views_30s_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_complete_views_30s_repeat_views', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_10s', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_10s_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_10s_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_10s_autoplayed', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_10s_click_to_play', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_10s_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_views_10s_repeat', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_video_view_time', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_views_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_views_logout', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_views_logged_in_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_views_logged_in_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'page_views_external_referrals', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_views_by_profile_tab_total', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_views_by_profile_tab_logged_in_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_views_by_internal_referer_logged_in_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_views_by_site_logged_in_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_views_by_age_gender_logged_in_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_views_by_referers_logged_in_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_content_activity_by_action_type_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_content_activity_by_age_gender_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_content_activity_by_city_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_content_activity_by_country_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_content_activity_by_locale_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_content_activity_by_action_type', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_daily_video_ad_break_ad_impressions_by_crosspost_status', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_daily_video_ad_break_cpm_by_crosspost_status', field_type = 'STRING'),
        bigquery.SchemaField(name = 'page_daily_video_ad_break_earnings_by_crosspost_status', field_type = 'STRING')                
        ]
    )
    
    upload_bigquery_data(frame, tabla, fb_page_insights_table, job_config)
    #print(f"{tabla} data uploaded successfully. CHECK")


def get_page_insights(since, until, token, id_fb_user, fb_page_insights_table):


    """
    Page general insights
    """

    # URL for general insights
    page_insights_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/insights?metric=page_total_actions,page_cta_clicks_logged_in_total,page_cta_clicks_logged_in_unique,page_call_phone_clicks_logged_in_unique,page_get_directions_clicks_logged_in_unique,page_website_clicks_logged_in_unique,page_website_clicks_by_site_logged_in_unique,page_engaged_users,page_post_engagements,page_consumptions,page_consumptions_unique,page_consumptions_by_consumption_type,page_places_checkin_total,page_places_checkin_total_unique,page_places_checkin_mobile,page_places_checkin_mobile_unique,page_negative_feedback,page_negative_feedback_unique,page_negative_feedback_by_type,page_negative_feedback_by_type_unique,page_positive_feedback_by_type_unique,page_fans_online,page_fans_online_per_day,page_fan_adds_by_paid_non_paid_unique,page_impressions,page_impressions_unique,page_impressions_paid,page_impressions_paid_unique,page_impressions_viral,page_impressions_viral_unique,page_impressions_nonviral,page_impressions_nonviral_unique,page_impressions_by_story_type_unique,page_impressions_frequency_distribution,page_impressions_viral_frequency_distribution,page_posts_impressions,page_posts_impressions_unique,page_posts_impressions_paid,page_posts_impressions_paid_unique,page_posts_impressions_organic,page_posts_impressions_organic,page_posts_served_impressions_organic_unique,page_posts_impressions_viral,page_posts_impressions_viral_unique,page_posts_impressions_nonviral,page_posts_impressions_nonviral_unique,page_posts_impressions_frequency_distribution&since={since}&until={until}&period=day&access_token={token}'
    
    # Function call get_info_from_url to return json
    info = get_info_from_url(page_insights_url, "page_insights_url")

    #function call page_ins sending json for do dataframe
    general_frame = page_insights_to_dataframe(info, id_fb_user)

    #array order and names of columns 
    keys = ['id_fb_user','end_time','process_timestamp','page_total_actions','page_cta_clicks_logged_in_total','page_cta_clicks_logged_in_unique','page_call_phone_clicks_logged_in_unique','page_get_directions_clicks_logged_in_unique','page_website_clicks_logged_in_unique','page_website_clicks_by_site_logged_in_unique','page_engaged_users','page_post_engagements','page_consumptions','page_consumptions_unique','page_consumptions_by_consumption_type','page_places_checkin_total','page_places_checkin_total_unique','page_places_checkin_mobile','page_places_checkin_mobile_unique','page_negative_feedback','page_negative_feedback_unique','page_negative_feedback_by_type','page_negative_feedback_by_type_unique','page_positive_feedback_by_type','page_positive_feedback_by_type_unique','page_fans_online','page_fans_online_per_day','page_fan_adds_by_paid_non_paid_unique','page_impressions','page_impressions_unique','page_impressions_paid','page_impressions_paid_unique','page_impressions_organic','page_impressions_organic_unique','page_impressions_viral','page_impressions_viral_unique','page_impressions_nonviral','page_impressions_nonviral_unique','page_impressions_by_story_type_unique','page_impressions_frequency_distribution','page_impressions_viral_frequency_distribution','page_posts_impressions','page_posts_impressions_unique','page_posts_impressions_paid','page_posts_impressions_paid_unique','page_posts_impressions_organic','page_posts_impressions_organic_unique','page_posts_served_impressions_organic_unique','page_posts_impressions_viral','page_posts_impressions_viral_unique','page_posts_impressions_nonviral','page_posts_impressions_nonviral_unique','page_posts_impressions_frequency_distribution']
    
    #function call frame_validate to add keys wich doesn't exist 
    general_frame = frame_validate(keys, general_frame)
    
    #specify the type of columns
    general_frame = general_frame.astype({'page_cta_clicks_logged_in_total':'str','page_cta_clicks_logged_in_unique':'str','page_website_clicks_by_site_logged_in_unique':'str','page_consumptions_by_consumption_type':'str','page_negative_feedback_by_type':'str','page_negative_feedback_by_type_unique':'str','page_positive_feedback_by_type':'str','page_positive_feedback_by_type_unique':'str','page_fans_online':'str','page_fan_adds_by_paid_non_paid_unique':'str','page_impressions_by_story_type_unique':'str','page_impressions_frequency_distribution':'str','page_impressions_viral_frequency_distribution':'str','page_posts_impressions_frequency_distribution':'str'})


    """
    Page actions insights
    """

    # URL for page actions insights
    page_actions_url=f'https://graph.facebook.com/v15.0/{id_fb_user}/insights?metric=page_actions_post_reactions_like_total%2Cpage_actions_post_reactions_love_total%2Cpage_actions_post_reactions_wow_total%2Cpage_actions_post_reactions_haha_total%2Cpage_actions_post_reactions_sorry_total%2Cpage_actions_post_reactions_anger_total%2Cpage_actions_post_reactions_total&since={since}&until={until}&period=day&access_token={token}'
    
    #function call get_info_from_url to return json 
    info = get_info_from_url(page_actions_url, "page_actions")
    
    #function call page_ins sending json for do dataframe
    actions_frame = page_insights_to_dataframe(info, id_fb_user)

    #array order and names of columns 
    keys = ['id_fb_user','end_time','process_timestamp','page_actions_post_reactions_like_total','page_actions_post_reactions_love_total','page_actions_post_reactions_wow_total','page_actions_post_reactions_haha_total','page_actions_post_reactions_sorry_total','page_actions_post_reactions_anger_total','page_actions_post_reactions_total']
    
    #function call frame_validate to add keys wich doesn't exist 
    actions_frame = frame_validate(keys, actions_frame)
    
    #specify the type of columns
    actions_frame = actions_frame.astype({'page_actions_post_reactions_total':'str'})

    # Create a final dataframe by joining general and actions dataframes
    final_frame = pd.merge(general_frame, actions_frame, on = ['id_fb_user','end_time'], how = 'left')

    # Drop duplicate columns in dataframe
    final_frame.drop([col for col in final_frame.columns if '_y' in col], axis = 1, inplace = True)

    # Rename duplicate columnms from the join phase
    final_frame.rename(columns = {"process_timestamp_x": "process_timestamp"}, inplace = True)


    """
    Page demographics insights
    """

    # URL for page demograhics insights
    page_demographics_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/insights?metric=page_fans,page_fans_locale,page_fans_city,page_fans_country,page_fans_gender_age,page_fan_adds_unique,page_fans_by_like_source,page_fans_by_like_source_unique,page_fan_removes,page_fan_removes_unique,page_fans_by_unlike_source_unique&since={since}&until={until}&period=day&access_token={token}'
    
    #function call get_info_from_url to return json 
    info = get_info_from_url(page_demographics_url, "page_demogra")
    
    #function call page_ins sending json for do dataframe
    demographics_frame = page_insights_to_dataframe(info,id_fb_user)
    
    # array order and names of columns 
    keys=['id_fb_user','end_time','process_timestamp','page_fans','page_fans_locale','page_fans_city','page_fans_country','page_fans_gender_age','page_fan_adds_unique','page_fans_by_like_source','page_fans_by_like_source_unique','page_fan_removes','page_fan_removes_unique','page_fans_by_unlike_source_unique']
    
    #function call frame_validate to add keys wich doesn't exist 
    demographics_frame = frame_validate(keys,demographics_frame)
    
    #specify the type of columns
    demographics_frame = demographics_frame.astype({'page_fans_locale':'str','page_fans_city':'str','page_fans_country':'str','page_fans_gender_age':'str','page_fans_by_like_source':'str','page_fans_by_like_source_unique':'str','page_fans_by_unlike_source_unique':'str'})

    # Join final and demographics dataframes
    final_frame = pd.merge(final_frame, demographics_frame, on = ['id_fb_user','end_time'], how = 'left')

    # Drop duplicate columns in dataframe
    final_frame.drop([col for col in final_frame.columns if '_y' in col], axis = 1, inplace = True)

    # Rename duplicate columnms from the join phase
    final_frame.rename(columns = {"process_timestamp_x": "process_timestamp"}, inplace = True)


    """
    Page video views insights
    """ 

    # URL for page video_views insights
    page_video_views_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/insights?metric=page_video_views,page_video_views_paid,page_video_views_organic,page_video_views_by_paid_non_paid,page_video_views_autoplayed,page_video_views_click_to_play,page_video_views_unique,page_video_repeat_views,page_video_complete_views_30s,page_video_complete_views_30s_paid,page_video_complete_views_30s_organic,page_video_complete_views_30s_autoplayed,page_video_complete_views_30s_click_to_play,page_video_complete_views_30s_unique,page_video_complete_views_30s_repeat_views,page_video_views_10s,page_video_views_10s_paid,page_video_views_10s_organic,page_video_views_10s_autoplayed,page_video_views_10s_click_to_play,page_video_views_10s_unique,page_video_views_10s_repeat,page_video_view_time&since={since}&until={until}&period=day&access_token={token}'
    
    #function call get_info_from_url to return json 
    info = get_info_from_url(page_video_views_url, "page_video_views")
    
    # function call page_ins sending json for do dataframe
    video_views_frame = page_insights_to_dataframe(info,id_fb_user)
    
    #array order and names of columns 
    keys=['id_fb_user','end_time','process_timestamp','page_video_views','page_video_views_paid','page_video_views_organic','page_video_views_by_paid_non_paid','page_video_views_autoplayed','page_video_views_click_to_play','page_video_views_unique','page_video_repeat_views','page_video_complete_views_30s','page_video_complete_views_30s_paid','page_video_complete_views_30s_organic','page_video_complete_views_30s_autoplayed','page_video_complete_views_30s_click_to_play','page_video_complete_views_30s_unique','page_video_complete_views_30s_repeat_views','page_video_views_10s','page_video_views_10s_paid','page_video_views_10s_organic','page_video_views_10s_autoplayed','page_video_views_10s_click_to_play','page_video_views_10s_unique','page_video_views_10s_repeat','page_video_view_time']
    
    #function call frame_validate to add keys wich doesn't exist 
    video_views_frame = frame_validate(keys, video_views_frame)
    
    #specify the type of columns
    video_views_frame = video_views_frame.astype({'page_video_views_by_paid_non_paid':'str'}) 
    
    # Join final and video_views frames
    final_frame = pd.merge(final_frame, video_views_frame, on = ['id_fb_user','end_time'], how = 'left')

    # Drop duplicate columns in dataframe
    final_frame.drop([col for col in final_frame.columns if '_y' in col], axis = 1, inplace = True)

    # Rename duplicate columnms from the join phase
    final_frame.rename(columns = {"process_timestamp_x": "process_timestamp"}, inplace = True)


    """
    Page views insights
    """  
    
    # URL for page views insights
    page_views_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/insights?metric=page_views_total,page_views_logout,page_views_logged_in_total,page_views_logged_in_unique,page_views_external_referrals,page_views_by_profile_tab_total,page_views_by_profile_tab_logged_in_unique,page_views_by_internal_referer_logged_in_unique,page_views_by_site_logged_in_unique,page_views_by_age_gender_logged_in_unique,page_views_by_referers_logged_in_unique&since={since}&until={until}&period=day&access_token={token}'
    #function call get_info_from_url to return json 
    info = get_info_from_url(page_views_url,"page_views")
    
    #function call page_ins sending json for do dataframe
    page_views_frame = page_insights_to_dataframe(info,id_fb_user)
    
    #array order and names of columns 
    keys=['id_fb_user','end_time','process_timestamp','page_views_total','page_views_logout','page_views_logged_in_total','page_views_logged_in_unique','page_views_external_referrals','page_views_by_profile_tab_total','page_views_by_profile_tab_logged_in_unique','page_views_by_internal_referer_logged_in_unique','page_views_by_site_logged_in_unique','page_views_by_age_gender_logged_in_unique','page_views_by_referers_logged_in_unique']
    
    #function call frame_validate to add keys wich doesn't exist 
    page_views_frame = frame_validate(keys, page_views_frame)
    
    #specify the type of columns
    page_views_frame = page_views_frame.astype({'page_views_external_referrals':'str','page_views_by_profile_tab_total':'str','page_views_by_profile_tab_logged_in_unique':'str','page_views_by_internal_referer_logged_in_unique':'str','page_views_by_site_logged_in_unique':'str','page_views_by_age_gender_logged_in_unique':'str','page_views_by_referers_logged_in_unique':'str'})
    
    # Join final and video_views frames
    final_frame = pd.merge(final_frame, page_views_frame, on = ['id_fb_user','end_time'], how = 'left')

    # Drop duplicate columns in dataframe
    final_frame.drop([col for col in final_frame.columns if '_y' in col], axis = 1, inplace = True)

    # Rename duplicate columnms from the join phase
    final_frame.rename(columns = {"process_timestamp_x": "process_timestamp"}, inplace = True)


    """
    Page content insights
    """  
    
    # URL for page views insights
    page_content_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/insights?metric=page_content_activity_by_action_type_unique,page_content_activity_by_age_gender_unique,page_content_activity_by_city_unique,page_content_activity_by_country_unique,page_content_activity_by_locale_unique,page_content_activity_by_action_type&since={since}&until={until}&period=day&access_token={token}'
    
    #function call get_info_from_url to return json 
    info = get_info_from_url(page_content_url,"page_content")
    
    #function call page_ins sending json for do dataframe
    content_frame = page_insights_to_dataframe(info,id_fb_user)
    
    #array order and names of columns 
    keys=['id_fb_user','end_time','process_timestamp','page_content_activity_by_action_type_unique','page_content_activity_by_age_gender_unique','page_content_activity_by_city_unique','page_content_activity_by_country_unique','page_content_activity_by_locale_unique','page_content_activity_by_action_type']
    
    #function call frame_validate to add keys wich doesn't exist 
    content_frame = frame_validate(keys, content_frame)
    
    #specify the type of columns
    content_frame = content_frame.astype({'page_content_activity_by_action_type_unique':'str','page_content_activity_by_age_gender_unique':'str','page_content_activity_by_city_unique':'str','page_content_activity_by_country_unique':'str','page_content_activity_by_locale_unique':'str','page_content_activity_by_action_type':'str'})
    
    # Join final and page content frames
    final_frame = pd.merge(final_frame, content_frame, on = ['id_fb_user','end_time'], how = 'left')

    # Drop duplicate columns in dataframe
    final_frame.drop([col for col in final_frame.columns if '_y' in col], axis = 1, inplace = True)

    # Rename duplicate columnms from the join phase
    final_frame.rename(columns = {"process_timestamp_x": "process_timestamp"}, inplace = True)



    """
    Page video_ad_breaks insights
    """     
    
    # URL for page video ad breaks
    page_video_ad_breaks_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/insights?metric=page_daily_video_ad_break_ad_impressions_by_crosspost_status,page_daily_video_ad_break_cpm_by_crosspost_status,page_daily_video_ad_break_earnings_by_crosspost_status&since={since}&until={until}&period=day&access_token={token}'
    
    # Function call to get_info_from_url in json 
    info = get_info_from_url(page_video_ad_breaks_url, "page_Video_Ad_Breaks")
    
    #function call page_ins sending json for do dataframe
    ad_breaks_frame = page_insights_to_dataframe(info, id_fb_user)
    
    #array order and names of columns 
    keys=['id_fb_user','end_time','process_timestamp','page_daily_video_ad_break_ad_impressions_by_crosspost_status','page_daily_video_ad_break_cpm_by_crosspost_status','page_daily_video_ad_break_earnings_by_crosspost_status']
    
    #function call frame_validate to add keys wich doesn't exist 
    ad_breaks_frame = frame_validate(keys, ad_breaks_frame)
    
    #specify the type of columns
    ad_breaks_frame = ad_breaks_frame.astype({'page_daily_video_ad_break_ad_impressions_by_crosspost_status':'str','page_daily_video_ad_break_cpm_by_crosspost_status':'str','page_daily_video_ad_break_earnings_by_crosspost_status':'str'})
    
    # Join final and ad_breaks frames
    final_frame = pd.merge(final_frame, ad_breaks_frame, on = ['id_fb_user','end_time'], how = 'left')

    # Drop duplicate columns in dataframe
    final_frame.drop([col for col in final_frame.columns if '_y' in col], axis = 1, inplace = True)

    # Rename duplicate columnms from the join phase
    final_frame.rename(columns = {"process_timestamp_x": "process_timestamp"}, inplace = True)


    final_frame['process_timestamp'] = pd.to_datetime(final_frame['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')

    # Upload final frame to fb_page_insights table
    bigquery_config_page_insights(final_frame, "page_insights", fb_page_insights_table)



"""
Post info and insights
"""

def get_info_posts(url, post_id, id_fb_user):
    
    # Call get_info_from_url function to get the response from the API
    info = get_info_from_url(url, "get_info_posts")

    # Instantiate an empty dict
    element_dict = {}

    # Get keys from API response
    info_keys = info.keys()

    ##### BEGIN DEBUG: Print response keys
    #print('info keys {}'.format(info_keys))
    ##### END DEBUG

    # If response contains error, dismiss
    if ('error' in info_keys):
        return -1

    # Iterate over each element in the response
    for element in info['data']:
        
        # Append the element name and the value in the empty dict
        element_dict[element['name']] = element['values'][0]['value']
    
    # Add id_fb_user and process timestamp to the empty dict
    element_dict['id_fb_user'] = id_fb_user
    element_dict['process_timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    ##### BEGIN TEST: Keep only the post_id
    fb_page_id, new_id_post = post_id.split('_')
    element_dict['id_post'] = new_id_post
    ##### END TEST

    # Save the post_id to the element_dict
    #element_dict['post_id'] = post_id

    return element_dict

def bigquery_config_post_insights(df, tabla, fb_post_insights_table):
    
    client = bigquery.Client()
    
    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
        #Specify the type of columns whose type cannot be auto-detected. For
        #example the "page_fans_locale" column uses pandas dtype "object", so its
        #data type is ambiguous.
        bigquery.SchemaField(name = 'id_fb_user', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'id_post', field_type = 'STRING'),
        bigquery.SchemaField(name = 'end_time', field_type = 'TIMESTAMP'),
        bigquery.SchemaField(name = 'process_timestamp', field_type = 'TIMESTAMP'),
        bigquery.SchemaField(name = 'post_engaged_users', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_negative_feedback', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_negative_feedback_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_negative_feedback_by_type', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_negative_feedback_by_type_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_engaged_fan', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_clicks', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_clicks_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_paid_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_fan', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_fan_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_fan_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_fan_paid_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_organic_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_viral', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_viral_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_nonviral', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_nonviral_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_impressions_by_story_type', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_impressions_by_story_type_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_reactions_like_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_reactions_love_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_reactions_wow_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_reactions_haha_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_reactions_sorry_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_reactions_anger_total', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_reactions_by_type_total', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_activity', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_activity_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_activity_by_action_type', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_activity_by_action_type_unique', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_video_complete_views_30s_clicked_to_play', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_complete_views_30s_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_complete_views_30s_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_complete_views_30s_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_avg_time_watched', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_complete_views_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_complete_views_organic_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_complete_views_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_complete_views_paid_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_retention_graph', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_video_retention_graph_clicked_to_play', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_video_retention_graph_autoplayed', field_type = 'STRING'),
        bigquery.SchemaField(name = 'post_video_views_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_organic_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_paid_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_length', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_autoplayed', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_clicked_to_play', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_15s', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_60s_excludes_shorter', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_10s', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_10s_unique', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_10s_autoplayed', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_10s_clicked_to_play', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_10s_organic', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_10s_paid', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_10s_sound_on', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_views_sound_on', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_view_time', field_type = 'INTEGER'),
        bigquery.SchemaField(name = 'post_video_view_time_organic', field_type = 'INTEGER')
        ]
    )

    # Upload data to BigQuery
    upload_bigquery_data(df, tabla, fb_post_insights_table, job_config)
    #print(f"{tabla} data uploaded successfully. CHECK")

def get_post_insights(token, since, post_id_list, id_fb_user, fb_post_insights_table):

    """
    Post insights
    """

    # Declare columns to order dataframe
    post_insights_keys = ['id_fb_user','id_post', 'end_time', 'process_timestamp','post_engaged_users','post_negative_feedback','post_negative_feedback_unique','post_negative_feedback_by_type','post_negative_feedback_by_type_unique','post_engaged_fan','post_clicks','post_clicks_unique','post_impressions','post_impressions_unique','post_impressions_paid','post_impressions_paid_unique','post_impressions_fan','post_impressions_fan_unique','post_impressions_fan_paid','post_impressions_fan_paid_unique','post_impressions_organic','post_impressions_organic_unique','post_impressions_viral','post_impressions_viral_unique','post_impressions_nonviral','post_impressions_nonviral_unique','post_impressions_by_story_type','post_impressions_by_story_type_unique', 'post_reactions_like_total', 'post_reactions_love_total', 'post_reactions_wow_total', 'post_reactions_haha_total', 'post_reactions_sorry_total', 'post_reactions_anger_total', 'post_reactions_by_type_total', 'post_activity','post_activity_unique','post_activity_by_action_type','post_activity_by_action_type_unique', 'post_video_complete_views_30s_clicked_to_play','post_video_complete_views_30s_paid','post_video_complete_views_30s_organic','post_video_complete_views_30s_unique','post_video_avg_time_watched','post_video_complete_views_organic','post_video_complete_views_organic_unique','post_video_complete_views_paid','post_video_complete_views_paid_unique','post_video_retention_graph','post_video_retention_graph_clicked_to_play','post_video_retention_graph_autoplayed','post_video_views_organic','post_video_views_organic_unique','post_video_views_paid','post_video_views_paid_unique','post_video_length','post_video_views','post_video_views_unique','post_video_views_autoplayed','post_video_views_clicked_to_play','post_video_views_15s','post_video_views_60s_excludes_shorter','post_video_views_10s','post_video_views_10s_unique','post_video_views_10s_autoplayed','post_video_views_10s_clicked_to_play','post_video_views_10s_organic','post_video_views_10s_paid','post_video_views_10s_sound_on','post_video_views_sound_on','post_video_view_time','post_video_view_time_organic']
    
    # Instantiate an empty list for storing responses for each id
    post_list = []
    
    # Iterate over each post in the list 
    for post_id in post_id_list:

        #Build the corresponding URL to call the endpoint
        url = f'https://graph.facebook.com/v15.0/{post_id}/insights?metric=post_engaged_users%2Cpost_negative_feedback%2Cpost_negative_feedback_unique%2Cpost_negative_feedback_by_type%2Cpost_negative_feedback_by_type_unique%2Cpost_engaged_fan%2Cpost_clicks%2Cpost_clicks_unique%2Cpost_impressions%2Cpost_impressions_unique%2Cpost_impressions_paid%2Cpost_impressions_paid_unique%2Cpost_impressions_fan%2Cpost_impressions_fan_unique%2Cpost_impressions_fan_paid%2Cpost_impressions_fan_paid_unique%2Cpost_impressions_organic%2Cpost_impressions_organic_unique%2Cpost_impressions_viral%2Cpost_impressions_viral_unique%2Cpost_impressions_nonviral%2Cpost_impressions_nonviral_unique%2Cpost_impressions_by_story_type%2Cpost_impressions_by_story_type_unique%2Cpost_reactions_like_total%2Cpost_reactions_love_total%2Cpost_reactions_wow_total%2Cpost_reactions_haha_total%2Cpost_reactions_sorry_total%2Cpost_reactions_anger_total%2Cpost_reactions_by_type_total%2Cpost_activity%2Cpost_activity_unique%2Cpost_activity_by_action_type%2Cpost_activity_by_action_type_unique%2Cpost_video_complete_views_30s_clicked_to_play%2Cpost_video_complete_views_30s_paid%2Cpost_video_complete_views_30s_organic%2Cpost_video_complete_views_30s_unique%2Cpost_video_avg_time_watched%2Cpost_video_complete_views_organic%2Cpost_video_complete_views_organic_unique%2Cpost_video_complete_views_paid%2Cpost_video_complete_views_paid_unique%2Cpost_video_retention_graph%2Cpost_video_retention_graph_clicked_to_play%2Cpost_video_retention_graph_autoplayed%2Cpost_video_views_organic%2Cpost_video_views_organic_unique%2Cpost_video_views_paid%2Cpost_video_views_paid_unique%2Cpost_video_length%2Cpost_video_views%2Cpost_video_views_unique%2Cpost_video_views_autoplayed%2Cpost_video_views_clicked_to_play%2Cpost_video_views_15s%2Cpost_video_views_60s_excludes_shorter%2Cpost_video_views_10s%2Cpost_video_views_10s_unique%2Cpost_video_views_10s_autoplayed%2Cpost_video_views_10s_clicked_to_play%2Cpost_video_views_10s_organic%2Cpost_video_views_10s_paid%2Cpost_video_views_10s_sound_on%2Cpost_video_views_sound_on%2Cpost_video_view_time%2Cpost_video_view_time_organic&period=lifetime&access_token={token}'

        # DEBUG Print URL and post_id
        #print(url)
        #print('Processing post_insights on post_id {}'.format(post_id))
        # END DEBUG

        # Append the current post_id to the post_list
        post_insights = get_info_posts(url, post_id, id_fb_user)

        # Validate if insights response is valid
        if post_insights == -1:
            continue
        else:
            post_list.append(post_insights)

    # Create a dataframe using all the posts data
    post_insights_df = pd.json_normalize(post_list, max_level = 0)

    #df.fillna(np.nan)
    # Add end_time dataframe
    post_insights_df.insert(0, 'end_time', since)
    post_insights_df['end_time'] = pd.to_datetime(post_insights_df['end_time'], format = '%Y-%m-%d')
    post_insights_df['id_post'] = post_insights_df['id_post'].astype(str)
    
    post_insights_df = frame_validate(post_insights_keys, post_insights_df)
    #df.to_csv(f"post_impresions{since}.csv")

    post_insights_df = post_insights_df.astype({'id_post':'str','post_negative_feedback_by_type':'str','post_negative_feedback_by_type_unique':'str','post_impressions_by_story_type':'str','post_impressions_by_story_type_unique':'str', 'post_reactions_by_type_total':'str', 'post_activity_by_action_type':'str', 'post_activity_by_action_type_unique':'str', 'post_video_retention_graph':'str', 'post_video_retention_graph_clicked_to_play':'str', 'post_video_retention_graph_autoplayed':'str'})
    post_insights_df['process_timestamp'] = pd.to_datetime(post_insights_df['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')

    #post_insights_df.info()
    #print(post_insights_df.head(10))
    #kb_input = input('Text to continue ')

    bigquery_config_post_insights(post_insights_df[post_insights_keys], "post_insights", fb_post_insights_table)

    return


def delete_duplicate_data_bigquery_tables(delete_date, fb_page_info_table,fb_page_insights_table,fb_albums_info_table,fb_photo_albums_info_table,fb_video_lists_info_table,fb_video_info_table,fb_live_video_info_table, fb_post_info_table, fb_post_insights_table):
    
    client = bigquery.Client()
    #delete_date = datetime.today() - timedelta(days = 1)
    #delete_date = str(delete_date.date())
    
    try:

        # Set delete queries
        # Page and post info
        sqlquery_delete_fb_page_info_table = """DELETE FROM {} WHERE DATE(end_time) = '{}'""".format(fb_page_info_table, delete_date)
        sqlquery_delete_fb_post_info_table = """DELETE FROM {} WHERE DATE(created_time) = '{}'""".format(fb_post_info_table, delete_date)

        # Page and post insights
        sqlquery_delete_fb_page_insights_table = """DELETE FROM {} WHERE DATE(end_time) = '{}'""".format(fb_page_insights_table, delete_date)
        sqlquery_delete_fb_post_insights_table = """DELETE FROM {} WHERE DATE(end_time) = '{}'""".format(fb_post_insights_table, delete_date)
        
        # Other elements info
        sqlquery_delete_fb_albums_info_table = """DELETE FROM {} WHERE DATE(created_time) = '{}'""".format(fb_albums_info_table, delete_date)
        sqlquery_delete_fb_photo_albums_info_table = """DELETE FROM {} WHERE DATE(created_time) = '{}'""".format(fb_photo_albums_info_table, delete_date)
        sqlquery_delete_fb_video_lists_info_table = """DELETE FROM {} WHERE DATE(creation_time) = '{}'""".format(fb_video_lists_info_table, delete_date)
        sqlquery_delete_fb_video_info_table = """DELETE FROM {} WHERE DATE(created_time) = '{}'""".format(fb_video_info_table, delete_date)
        sqlquery_delete_fb_live_video_info_table = """DELETE FROM {} WHERE DATE(creation_time) = '{}'""".format(fb_live_video_info_table, delete_date)

        
        # Execute delete queries
        # Page and post info
        query_job_delete_fb_page_info_table = client.query(sqlquery_delete_fb_page_info_table)
        query_job_delete_fb_page_insights_table = client.query(sqlquery_delete_fb_page_insights_table)

        # Page and post insights
        query_job_delete_fb_post_insights_table = client.query(sqlquery_delete_fb_post_insights_table)
        query_job_delete_fb_post_info_table = client.query(sqlquery_delete_fb_post_info_table)

        # Other elements info
        query_job_delete_fb_albums_info_table = client.query(sqlquery_delete_fb_albums_info_table)
        query_job_delete_fb_photo_albums_info_table= client.query(sqlquery_delete_fb_photo_albums_info_table)
        query_job_delete_fb_video_lists_info_table = client.query(sqlquery_delete_fb_video_lists_info_table)
        query_job_delete_fb_video_info_table = client.query(sqlquery_delete_fb_video_info_table)
        query_job_delete_fb_live_video_info_table = client.query(sqlquery_delete_fb_live_video_info_table)
        
    except Exception as e:
        print(e)
        slack_update = 'ERROR: fb_data_update job failed on delete_fb_tables step with exception {}'.format(e)
        print(slack_update)
        post_slack_updates(slack_update)
        raise

    
def get_id_list(id_fb_user, since, until, token, url, table):
    
    # Instantiate an empty list to store all ids from response
    id_list = []
    id_url = url
    
    # Set an infinite while to iterate over cursors from response
    while id_url:

        # Try to get a response using the URL
        try:        
            response = get_info_from_url(id_url, table)

            ##### DEBUG: show response
            #print(response, '\n')
            ##### END DEBUG

            #if response['data'] == []:

                #print('continuing...')
                #id_url = response['paging']['next']
                #continue

            counter = 0
            response_len=''
            # Iterate over each element in response
            for element in response['data']:

                # If the id from the current element is not in the id_list
                if element['id'] not in id_list:

                    # Append the current element id to the list
                    id_list.append(element['id'])
                else:
                    counter = counter + 1
                    response_len = len(response['data'])
            
            #print('counter',counter)
            #print('response_len',response_len)
            if counter == response_len:
                break
            


            # Set URL = next cursor
            id_url = response['paging']['next']
        
        # If something goes wrong
        except KeyError:
            break
        except Exception as e:
            # SEND ERROR DATA TO CONTROL TABLE IN BIGQUERY
            slack_update = f'ERROR: fb_data_coursors job failed on {table} phase with exception: {e}'
            print(slack_update)
            #post_slack_updates(slack_update)
            raise
    
    # Return the populated id list
    return id_list

def get_media_id_list(process_type,id_fb_user, since, until, token):
    
    """
    Get the post_id list
    """
    # Build the URL to request
    post_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/posts?since={since}&until={until}&access_token={token}'
    
    # Get the id list from the utility function
    posts_id_list = get_id_list(id_fb_user, since, until, token, post_url, 'post_list')
    print('post_id list complete with {} elements'. format(len(posts_id_list)))

    """
    Get the ads_post_id list
    """
    # Build the URL to request
    ads_post_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/ads_posts?since={since}&until={until}&access_token={token}'

    # Get the id list from the utility function
    ads_post_id_list = get_id_list(id_fb_user, since, until, token, ads_post_url, 'ads_post_list')
    print('ads_post_id list complete with {} elements'. format(len(ads_post_id_list)))

    """
    Get the albums_id list
    """
    # Build the URL to request
    albums_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/albums?access_token={token}'

    # Get the id list from the utility function
    albums_id_list=get_id_list(id_fb_user, since, until, token, albums_url, 'albums_list')
    print('albums_id list complete with {} elements'. format(len(albums_id_list)))
    

    """
    Get the albums_photo_id_list list: Iterate over the albums first
    """
    # Instantiate an empty list to add each id
    albums_photo_id_list=[]
    
    # Iterate over each album in the album_id_list
    for album_id in albums_id_list:

        # Build the URL to request
        albums_photo_url = f'https://graph.facebook.com/v15.0/{album_id}/photos?since={since}&until={until}&access_token={token}'
        
        # Get the id list from the utility function
        albums_ph_id_list = get_id_list(id_fb_user, since, until, token, albums_photo_url, 'photo_albums_list')

        # Append the current photo list
        albums_photo_id_list += albums_ph_id_list
    print('albums_photo_id_list list complete with {} elements'. format(len(albums_photo_id_list)))


    """
    Get the video_lists_id_list list
    """
    # Build the URL to request
    video_lists_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/video_lists?since=2022-03-01&until={since}&access_token={token}'

    # Get the id list from the utility function
    video_lists_id_list = get_id_list(id_fb_user, since, until, token, video_lists_url, ' ')
    print('video_lists_id list complete with {} elements'. format(len(video_lists_id_list)))
    

    """
    Get the vids_id_list list
    """
    # Build the URL to request
    vids_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/videos?since={since}&until={until}&access_token={token}'
    
    # Get the id list from the utility function
    vids_id_list = get_id_list(id_fb_user, since, until, token, vids_url, 'video_list')
    print('videos_id list complete with {} elements'. format(len(vids_id_list)))


    """
    Get the live_vids_id_list list
    """
    # Build the URL to request
    live_vids_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/live_videos?since={since}&until={until}&access_token={token}'
    
    # Get the id list from the utility function
    live_vids_id_list = get_id_list(id_fb_user, since, until, token, live_vids_url, 'live_video_list')
    print('live_videos_id list complete with {} elements'. format(len(live_vids_id_list)))
    print(live_vids_id_list)


    """
    Get the reels_vids_id_list list
    """
    # Build the URL to request
    reels_vids_url = f'https://graph.facebook.com/v15.0/{id_fb_user}/video_reels?since={since}&until={until}&access_token={token}'
    
    # Get the id list from the utility function
    reels_vids_id_list = get_id_list(id_fb_user, since, until, token, reels_vids_url, ' ')
    print('video_reels_id list complete with {} elements'. format(len(reels_vids_id_list)))

    #video_lists_video*******
    #video_lists_video_id_list=[]
    #for video_list_id in video_lists_id_list:
        #video_lists_video_url=f'https://graph.facebook.com/v15.0/{video_list_id}/videos?since={since}&until={until}&access_token={token}'
        #video_list_vid_id_list=get_id_list(id_fb_user,since,until,token,video_lists_video_url,'video_lists_video_id')
        #video_lists_video_id_list+=video_list_vid_id_list


    #indexed_videos
    #indexed_vids_url=f'https://graph.facebook.com/v15.0/{id_fb_user}/indexed_videos?since={since}&until={until}&access_token={token}'
    #indexed_vids_id_list=get_id_list(id_fb_user,since,until,token,indexed_vids_url,'indexed_videos_id')
    
    """
    Generate the all_id_list
    """
    
    if process_type=='daily':
    # Add each one of the previously generated lists to create the complete list of IDs
        all_id_list = posts_id_list + ads_post_id_list + albums_id_list + albums_photo_id_list + video_lists_id_list + vids_id_list+reels_vids_id_list
        print('all_id_list list complete with {} elements'. format(len(all_id_list)))

    elif process_type in ('weekly', 'monthly', 'daily-week'):
            # Add each one of the previously generated lists to create the complete list of IDs
        all_id_list = posts_id_list + ads_post_id_list + albums_photo_id_list + vids_id_list
        print('all_id_list list complete with {} elements'. format(len(all_id_list)))
    
        
    # Return all lists
    return posts_id_list, ads_post_id_list ,albums_id_list, albums_photo_id_list, video_lists_id_list, vids_id_list, live_vids_id_list, reels_vids_id_list, all_id_list

def unique_list(id_fb_user, lists):
    posts_unique_list = []
    for idelement in lists:
        if '_' in idelement:
            if idelement not in posts_unique_list:
                    posts_unique_list.append(idelement)
        else:
            idelement_concat = str(id_fb_user) + '_' + str(idelement)
            if idelement_concat not in posts_unique_list:
                    posts_unique_list.append(idelement_concat)
    
    return posts_unique_list
    
def bigquery_config_post_info(dframe, tabla, fb_post_info_table):

    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
            
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "page_fans_locale" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField(name ='id_fb_user',field_type ='INTEGER'),
            bigquery.SchemaField(name ='origin_id_post',field_type ='STRING'),
            bigquery.SchemaField(name ='id_post',field_type ='STRING'),
            bigquery.SchemaField(name ='process_timestamp',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='post_type',field_type ='STRING'),
            bigquery.SchemaField(name ='attachments_media_type',field_type ='STRING'),
            bigquery.SchemaField(name ='created_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='message',field_type ='STRING'),
            bigquery.SchemaField(name ='permalink_url',field_type ='STRING'),
            bigquery.SchemaField(name ='actions',field_type ='STRING'),
            bigquery.SchemaField(name ='can_reply_privately',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='full_picture',field_type ='STRING'),
            bigquery.SchemaField(name ='icon',field_type ='STRING'),
            bigquery.SchemaField(name ='instagram_eligibility',field_type ='STRING'),
            bigquery.SchemaField(name ='is_eligible_for_promotion',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_expired',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_hidden',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_inline_created',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_instagram_eligible',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_popular',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_published',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_spherical',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='multi_share_end_card',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='multi_share_optimized',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='promotable_id',field_type ='STRING'),
            bigquery.SchemaField(name ='promotion_status',field_type ='STRING'),
            bigquery.SchemaField(name ='status_type',field_type ='STRING'),
            bigquery.SchemaField(name ='subscribed',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='timeline_visibility',field_type ='STRING'),
            bigquery.SchemaField(name ='updated_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='video_buying_eligibility',field_type ='STRING'),
            bigquery.SchemaField(name ='from_name',field_type ='STRING'),
            bigquery.SchemaField(name ='shares_count',field_type = 'INTEGER'),
            bigquery.SchemaField(name ='comments_data',field_type ='STRING'),
            bigquery.SchemaField(name ='comments_summary_total_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='reactions_data',field_type ='STRING'),
            bigquery.SchemaField(name ='reactions_summary_total_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='privacy_allow',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_deny',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_description',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_friends',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_value',field_type ='STRING'),
            bigquery.SchemaField(name ='message_tags',field_type ='STRING'),
            bigquery.SchemaField(name ='admin_creator_category',field_type ='STRING'),
            bigquery.SchemaField(name ='admin_creator_link',field_type ='STRING'),
            bigquery.SchemaField(name ='admin_creator_name',field_type ='STRING'),
            bigquery.SchemaField(name ='admin_creator_namespace',field_type ='STRING'),
            bigquery.SchemaField(name ='admin_creator_id',field_type ='STRING'),
            bigquery.SchemaField(name ='application_category',field_type ='STRING'),
            bigquery.SchemaField(name ='application_link',field_type ='STRING'),
            bigquery.SchemaField(name ='application_name',field_type ='STRING'),
            bigquery.SchemaField(name ='application_namespace',field_type ='STRING'),
            bigquery.SchemaField(name ='application_id',field_type ='STRING'),
            bigquery.SchemaField(name ='properties',field_type ='STRING'),
            bigquery.SchemaField(name ='to_data',field_type ='STRING'),
            bigquery.SchemaField(name ='call_to_action_type',field_type ='STRING'),
            bigquery.SchemaField(name ='call_to_action_value_link',field_type ='STRING'),
            bigquery.SchemaField(name ='call_to_action_value_link_caption',field_type ='STRING'),
            bigquery.SchemaField(name ='call_to_action_value_link_title',field_type ='STRING'),
            bigquery.SchemaField(name ='call_to_action_value_link_description',field_type ='STRING'),
            bigquery.SchemaField(name ='call_to_action_value_link_format',field_type ='STRING')
            
        ]
    )

    # Upload data to bigquery
    upload_bigquery_data(dframe, tabla, fb_post_info_table, job_config)
    #print(f"{tabla} data uploaded successfully. CHECK")

def get_post_info(id_fb_user, since, until, token, posts_id_list, ads_post_id_list, fb_post_info_table):

    """
    Request post info 
    """

    # Instantiate an empty list for storing posts
    posts_datainfo = []
    
    # Iterate over each element in post_id_list
    for post_id in posts_id_list:
        
        # Build request URL with proper parameters
        post_url_info = f'https://graph.facebook.com/v15.0/{post_id}'+'?fields=from,shares,id,call_to_action,created_time,message,permalink_url,attachments{media_type},comments.limit(0).summary(total_count),reactions.limit(0).summary(total_count),actions,admin_creator,application,can_reply_privately,coordinates,full_picture,icon,instagram_eligibility,is_eligible_for_promotion,is_expired,is_hidden,is_inline_created,is_instagram_eligible,is_popular,is_published,is_spherical,message_tags,multi_share_end_card,multi_share_optimized,privacy,promotable_id,promotion_status,properties,status_type,subscribed,timeline_visibility,updated_time,video_buying_eligibility,to'+f'&access_token={token}'
        
        # Try to send the request to the API and Return JSON object of the response
        post_data = get_info_from_url(post_url_info, 'post_info')
        
        # Get keys from post_data dictionary
        post_exception_keys = post_data.keys()
        
        # Add "attachments column to dataframe"
        if 'attachments' in post_exception_keys:
            post_data['attachments'] = post_data['attachments']['data'][0]

        # Clean "message" column
        if 'message' in post_exception_keys:
            post_data['message'] = post_data['message'].replace('\n', ' ').replace('  ', ' ')

        # Clean created_time field from the response
        #if 'created_time' in post_exception_keys:
        #    post_data['created_time'] = post_data['created_time'][:-5]
        #    post_clean_timestamp = datetime.strptime(post_data['created_time'], "%Y-%m-%dT%H:%M:%S")
        #    post_data['created_time'] = str(datetime.strptime(str(post_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

        # Clean updated_time field from the response
        #if 'updated_time' in post_exception_keys:
        #    post_data['updated_time'] = post_data['updated_time'][:-5]
        #    post_up_clean_timestamp = datetime.strptime(post_data['updated_time'], "%Y-%m-%dT%H:%M:%S")
        #    post_data['updated_time'] = str(datetime.strptime(str(post_up_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
        
        # Add origin_post_id and post_id to the current element
        if 'id' in post_exception_keys:
            fb_page_id, new_post_id = post_data['id'].split('_')
            post_data['id'] = new_post_id
            fb_page_id, post_data['id_post'] = post_id.split('_')

        # Add current post to post list
        posts_datainfo.append(post_data)
    
    # Create dataframe
    # Transform json object to a dataframe
    posts_dataframe = pd.json_normalize(posts_datainfo)

    if (len(posts_dataframe)) > 0:
        # Add process timestamp and post_type to dataframe 
        posts_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
        posts_dataframe.insert(0, 'post_type', 'post') 

        
        # Rename ig_user_column
        posts_dataframe.rename(columns = {'from.id':'id_fb_user','id':'origin_id_post','attachments.media_type':'attachments_media_type','from.name':'from_name','shares.count':'shares_count','comments.data':'comments_data','comments.summary.total_count':'comments_summary_total_count','reactions.data':'reactions_data','reactions.summary.total_count':'reactions_summary_total_count','privacy.allow':'privacy_allow','privacy.deny':'privacy_deny','privacy.description':'privacy_description','privacy.friends':'privacy_friends','privacy.value':'privacy_value','admin_creator.category':'admin_creator_category','admin_creator.link':'admin_creator_link','admin_creator.name':'admin_creator_name','admin_creator.namespace':'admin_creator_namespace','admin_creator.id':'admin_creator_id','application.category':'application_category','application.link':'application_link','application.name':'application_name','application.namespace':'application_namespace','application.id':'application_id','to.data':'to_data','call_to_action.type':'call_to_action_type','call_to_action.value.link':'call_to_action_value_link','call_to_action.value.link_caption':'call_to_action_value_link_caption','call_to_action.value.link_title':'call_to_action_value_link_title','call_to_action.value.link_description':'call_to_action_value_link_description','call_to_action.value.link_format':'call_to_action_value_link_format'}, inplace = True)
        posts_dataframe['id_fb_user'] = id_fb_user
        #posts_dataframe['id_post'] = posts_id_list

    """
    Request ad-post info 
    """

    # Instantiate an empty list for ad posts
    ads_posts_datainfo = []
    
    # Iterate over each ad post in the list
    for ad_post_id in ads_post_id_list:

        # Build request URL with proper parameters
        ads_post_url_info=f'https://graph.facebook.com/v15.0/{ad_post_id}'+'?fields=from,shares,id,call_to_action,created_time,message,permalink_url,attachments{media_type},comments.limit(0).summary(total_count),reactions.limit(0).summary(total_count),actions,admin_creator,application,can_reply_privately,coordinates,full_picture,icon,instagram_eligibility,is_eligible_for_promotion,is_expired,is_hidden,is_inline_created,is_instagram_eligible,is_popular,is_published,is_spherical,message_tags,multi_share_end_card,multi_share_optimized,privacy,promotable_id,promotion_status,properties,status_type,subscribed,timeline_visibility,updated_time,video_buying_eligibility,to'+f'&access_token={token}'
        
        # Try to send the request to the API and Return JSON object of the response
        ad_post_data = get_info_from_url(ads_post_url_info,'ads_post_info')
        
        # Get keys from ad_post_data dictionary
        ads_post_exception_keys = ad_post_data.keys()
        
        # Add "attachments column to dataframe"
        if 'attachments' in ads_post_exception_keys:
            ad_post_data['attachments'] = ad_post_data['attachments']['data'][0]

        # Clean "message" column
        if 'message' in ads_post_exception_keys:
            ad_post_data['message'] = ad_post_data['message'].replace('\n', ' ').replace('  ', ' ')

        # Clean created_time field from the response
        #if 'created_time' in ads_post_exception_keys:
        #    ad_post_data['created_time'] = ad_post_data['created_time'][:-5]
        #    ad_post_clean_timestamp = datetime.strptime(ad_post_data['created_time'], "%Y-%m-%dT%H:%M:%S")
        #    ad_post_data['created_time'] = str(datetime.strptime(str(ad_post_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

        # Clean updated_time field from the response
        #if 'updated_time' in ads_post_exception_keys:
        #    ad_post_data['updated_time'] = ad_post_data['updated_time'][:-5]
        #    ad_post_up_clean_timestamp = datetime.strptime(ad_post_data['updated_time'], "%Y-%m-%dT%H:%M:%S")
        #    ad_post_data['updated_time'] = str(datetime.strptime(str(ad_post_up_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

        # Add origin_post_id and post_id to the current element
        if 'id' in ads_post_exception_keys:
            fb_page_id, new_post_id = ad_post_data['id'].split('_')
            ad_post_data['id'] = new_post_id
            fb_page_id, ad_post_data['id_post'] = ad_post_id.split('_')

        # Add current ad-post to ad-posts list
        ads_posts_datainfo.append(ad_post_data)
    
    # Create dataframe
    # Transform json object to a dataframe
    ads_posts_dataframe = pd.json_normalize(ads_posts_datainfo)
    
    # Add process timestamp and post_type to dataframe 
    if len(ads_posts_dataframe) > 0:
        ads_posts_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
        ads_posts_dataframe.insert(0, 'post_type', 'ad post')
    
    # Rename fb_user_column
    ads_posts_dataframe.rename(columns = {'from.id':'id_fb_user','id':'origin_id_post','attachments.media_type':'attachments_media_type','from.name':'from_name','shares.count':'shares_count','comments.data':'comments_data','comments.summary.total_count':'comments_summary_total_count','reactions.data':'reactions_data','reactions.summary.total_count':'reactions_summary_total_count','privacy.allow':'privacy_allow','privacy.deny':'privacy_deny','privacy.description':'privacy_description','privacy.friends':'privacy_friends','privacy.value':'privacy_value','admin_creator.category':'admin_creator_category','admin_creator.link':'admin_creator_link','admin_creator.name':'admin_creator_name','admin_creator.namespace':'admin_creator_namespace','admin_creator.id':'admin_creator_id','application.category':'application_category','application.link':'application_link','application.name':'application_name','application.namespace':'application_namespace','application.id':'application_id','to.data':'to_data','call_to_action.type':'call_to_action_type','call_to_action.value.link':'call_to_action_value_link','call_to_action.value.link_caption':'call_to_action_value_link_caption','call_to_action.value.link_title':'call_to_action_value_link_title','call_to_action.value.link_description':'call_to_action_value_link_description','call_to_action.value.link_format':'call_to_action_value_link_format'}, inplace = True)
    ads_posts_dataframe['id_fb_user'] = id_fb_user
    #ads_posts_dataframe['id_post'] = ads_post_id_list
    
    # Create unique dataframe
    # Column order to match database table
    post_column_order = ['id_fb_user','origin_id_post','id_post','process_timestamp','post_type','attachments_media_type',
                    'created_time','message','permalink_url','actions','can_reply_privately','full_picture',
                    'icon','instagram_eligibility','is_eligible_for_promotion','is_expired','is_hidden',
                    'is_inline_created','is_instagram_eligible','is_popular','is_published','is_spherical',
                    'multi_share_end_card','multi_share_optimized','promotable_id','promotion_status','status_type',
                    'subscribed','timeline_visibility','updated_time','video_buying_eligibility','from_name',
                    'shares_count','comments_data','comments_summary_total_count','reactions_data',
                    'reactions_summary_total_count','privacy_allow','privacy_deny','privacy_description',
                    'privacy_friends','privacy_value','message_tags','admin_creator_category','admin_creator_link',
                    'admin_creator_name','admin_creator_namespace','admin_creator_id','application_category',
                    'application_link','application_name','application_namespace','application_id','properties',
                    'to_data','call_to_action_type','call_to_action_value_link','call_to_action_value_link_caption',
                    'call_to_action_value_link_title','call_to_action_value_link_description',
                    'call_to_action_value_link_format']
    

    # Concat post and ad_post
    unique_posts_dataframe = pd.concat([posts_dataframe, ads_posts_dataframe])

    if (len(unique_posts_dataframe)) > 0:

        # Validate dataframe
        unique_posts_dataframe = frame_validate(post_column_order, unique_posts_dataframe)
        
        # Process columns to set as string
        unique_posts_columns_str = ['id_post', 'origin_id_post', 'post_type','attachments_media_type','message','permalink_url',
                                    'actions','full_picture','icon','instagram_eligibility','promotable_id',
                                    'promotion_status','status_type','timeline_visibility','video_buying_eligibility',
                                    'from_name','comments_data','reactions_data','privacy_allow','privacy_deny',
                                    'privacy_description','privacy_friends','privacy_value','message_tags',
                                    'admin_creator_category','admin_creator_link','admin_creator_name','admin_creator_namespace',
                                    'application_category','application_link','application_name','application_namespace',
                                    'properties','to_data','call_to_action_type','call_to_action_value_link','call_to_action_value_link_caption',
                                    'call_to_action_value_link_title','call_to_action_value_link_description','call_to_action_value_link_format','admin_creator_id','application_id']
        
        # 'comments_summary_total_count','reactions_summary_total_count'
        # Process columns to set as integer
        unique_posts_columns_int = ['comments_summary_total_count','reactions_summary_total_count']
        unique_posts_columns_bool = ['can_reply_privately','is_eligible_for_promotion','is_expired','is_hidden','is_inline_created','is_instagram_eligible','is_popular','is_published','is_spherical','multi_share_end_card','multi_share_optimized','subscribed']
        unique_posts_columns_times = ['created_time','updated_time']
        
        # Transform column types
        #unique_posts_dataframe[unique_posts_columns_str] = unique_posts_dataframe[unique_posts_columns_str].fillna('null').astype(str)
        unique_posts_dataframe[unique_posts_columns_str] = unique_posts_dataframe[unique_posts_columns_str].astype(str)
        unique_posts_dataframe[unique_posts_columns_int] = unique_posts_dataframe[unique_posts_columns_int].astype(int,errors='ignore')
        unique_posts_dataframe[unique_posts_columns_bool] = unique_posts_dataframe[unique_posts_columns_bool].astype(bool)
        unique_posts_dataframe['created_time'] = pd.to_datetime(unique_posts_dataframe['created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
        unique_posts_dataframe['updated_time'] = pd.to_datetime(unique_posts_dataframe['updated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
        unique_posts_dataframe['process_timestamp'] = pd.to_datetime(unique_posts_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')
        print(unique_posts_dataframe[unique_posts_columns_int])

        #unique_posts_dataframe=unique_posts_dataframe[post_column_order]
        #unique_posts_dataframe.to_csv('prueba_post.csv')
        
        # Upload data to BigQuery
        bigquery_config_post_info(unique_posts_dataframe[post_column_order], 'post_info', fb_post_info_table)

        # End function execution
        return
           
def bigquery_config_albums_info(dframe, tabla, fb_albums_info_table):
    
    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
            
            #Specify the type of columns whose type cannot be auto-detected. For
            # example the "page_fans_locale" column uses pandas dtype "object", so its
            # data type is ambiguous.          
            bigquery.SchemaField(name ='id_fb_user',field_type ='INTEGER'),
            bigquery.SchemaField(name ='id_media',field_type ='STRING'),
            bigquery.SchemaField(name ='process_timestamp',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='media_type',field_type ='STRING'),
            bigquery.SchemaField(name ='created_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='can_upload',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='link',field_type ='STRING'),
            bigquery.SchemaField(name ='name',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy',field_type ='STRING'),
            bigquery.SchemaField(name ='type',field_type ='STRING'),
            bigquery.SchemaField(name ='updated_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='cover_photo_created_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='cover_photo_name',field_type ='STRING'),
            bigquery.SchemaField(name ='cover_photo_id',field_type ='INTEGER'),
            bigquery.SchemaField(name ='from_name',field_type ='STRING'),
            bigquery.SchemaField(name ='likes_data',field_type ='STRING'),
            bigquery.SchemaField(name ='likes_paging_cursors_before',field_type ='STRING'),
            bigquery.SchemaField(name ='likes_paging_cursors_after',field_type ='STRING'),
            bigquery.SchemaField(name ='picture_data_is_silhouette',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='picture_data_url',field_type ='STRING'),
            bigquery.SchemaField(name ='description',field_type ='STRING'),
            bigquery.SchemaField(name ='backdated_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='backdated_time_granularity',field_type ='STRING'),
            bigquery.SchemaField(name ='location',field_type ='STRING'),
            bigquery.SchemaField(name ='place_name',field_type ='STRING'),
            bigquery.SchemaField(name ='place_location_latitude',field_type ='STRING'),
            bigquery.SchemaField(name ='place_location_longitude',field_type ='STRING'),
            bigquery.SchemaField(name ='place_id',field_type ='INTEGER')
        ]
    )

    # Call function to upload data
    upload_bigquery_data(dframe,tabla,fb_albums_info_table,job_config)
    #print(f"{tabla} data uploaded successfully. CHECK")

def bigquery_config_photo_albums_info(dframe,tabla,fb_photo_albums_info_table):
    client = bigquery.Client()
    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
            #Specify the type of columns whose type cannot be auto-detected. For
            # example the "page_fans_locale" column uses pandas dtype "object", so its
            # data type is ambiguous.          
            bigquery.SchemaField(name ='id_fb_user',field_type ='INTEGER'),
            bigquery.SchemaField(name ='id_media',field_type ='STRING'),
            bigquery.SchemaField(name ='process_timestamp',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='media_type',field_type ='STRING'),
            bigquery.SchemaField(name ='created_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='alt_text',field_type ='STRING'),
            bigquery.SchemaField(name ='can_backdate',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='can_delete',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='can_tag',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='height',field_type ='INTEGER'),
            bigquery.SchemaField(name ='icon',field_type ='STRING'),
            bigquery.SchemaField(name ='images',field_type ='STRING'),
            bigquery.SchemaField(name ='link',field_type ='STRING'),
            bigquery.SchemaField(name ='name',field_type ='STRING'),
            bigquery.SchemaField(name ='page_story_id',field_type ='STRING'),
            bigquery.SchemaField(name ='updated_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='webp_images',field_type ='STRING'),
            bigquery.SchemaField(name ='width',field_type ='INTEGER'),
            bigquery.SchemaField(name ='picture',field_type ='STRING'),
            bigquery.SchemaField(name ='album_created_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='album_name',field_type ='STRING'),
            bigquery.SchemaField(name ='album_id',field_type ='INTEGER'),
            bigquery.SchemaField(name ='from_name',field_type ='STRING'),
            bigquery.SchemaField(name ='alt_text_custom',field_type ='STRING'),
            bigquery.SchemaField(name ='name_tags',field_type ='STRING')

        ]
    )
    upload_bigquery_data(dframe,tabla,fb_photo_albums_info_table,job_config)
    #print(f"{tabla} data uploaded successfully. CHECK")

def photo_info(process_type,id_fb_user,since,until,token,albums_id_list,albums_photo_id_list,fb_albums_info_table,fb_photo_albums_info_table):
    
    if process_type=='daily':
        
        #Albums info
        albums_datainfo=[]
        
        for album_id in albums_id_list:

            # Build request URL with proper parameters
            albums_url_info=f'https://graph.facebook.com/v15.0/{album_id}'+'?fields=backdated_time,backdated_time_granularity,can_upload,count,cover_photo,created_time,description,event,from,id,link,location,name,place,privacy,type,updated_time,comments.limit(0),likes,picture{cache_key,height,is_silhouette,url,width}'+f'&access_token={token}'
            
            # Try to send the request to the API and Return JSON object of the response
            album_data = get_info_from_url(albums_url_info,'albums_info')
            
            # Get the keys from the album_data dict
            album_exception_keys = album_data.keys()
            
            #Replace some columns with values
            if 'id' in album_exception_keys:
                #album_data['id'] = str(id_fb_user)+"_"+str(album_data['id'])
                album_data['id'] = str(album_data['id'])
            
            #Clean name
            if 'name' in album_exception_keys:
                album_data['name'] = album_data['name'].replace('\n', ' ').replace('  ', ' ')
                
            # Clean timestamp objects from the response    
            #if 'cover_photo' in album_exception_keys:
            #    album_data['cover_photo']['created_time'] = album_data['cover_photo']['created_time'][:-5]
            #    album_cover_clean_timestamp = datetime.strptime(album_data['cover_photo']['created_time'], "%Y-%m-%dT%H:%M:%S")
            #    album_data['cover_photo']['created_time'] = str(datetime.strptime(str(album_cover_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
            #if 'created_time' in album_exception_keys:
            #    album_data['created_time'] = album_data['created_time'][:-5]
            #    album_clean_timestamp = datetime.strptime(album_data['created_time'], "%Y-%m-%dT%H:%M:%S")
            #    album_data['created_time'] = str(datetime.strptime(str(album_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
            #if 'updated_time' in album_exception_keys:
            #    album_data['updated_time'] = album_data['updated_time'][:-5]
            #    album_up_clean_timestamp = datetime.strptime(album_data['updated_time'], "%Y-%m-%dT%H:%M:%S")
            #    album_data['updated_time'] = str(datetime.strptime(str(album_up_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
            #if 'backdated_time' in album_exception_keys:
            #    album_data['backdated_time'] = album_data['backdated_time'][:-5]
            #    album_back_clean_timestamp = datetime.strptime(album_data['backdated_time'], "%Y-%m-%dT%H:%M:%S")
            #    album_data['backdated_time'] = str(datetime.strptime(str(album_back_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            
                        
            albums_datainfo.append(album_data)
            
        # Create dataframe
        # Column order to match database table
        album_column_order = ['id_fb_user','id_media','process_timestamp','media_type','created_time','can_upload',
                            'count','link','name','privacy','type','updated_time','cover_photo_created_time',
                            'cover_photo_name','cover_photo_id','from_name','likes_data','likes_paging_cursors_before',
                            'likes_paging_cursors_after','picture_data_is_silhouette','picture_data_url','description',
                            'backdated_time','backdated_time_granularity','location','place_name',
                            'place_location_latitude','place_location_longitude','place_id']
        
        # Transform json object to a dataframe
        albums_dataframe = pd.json_normalize(albums_datainfo)
        
        if (len(albums_dataframe))>0:
            # Add process timestamp and post_type to dataframe
            albums_dataframe.insert(0,'media_type','album')
            albums_dataframe.insert(0,'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))

            # Rename columns
            albums_dataframe.rename(columns = {'from.id':'id_fb_user','id':'id_media','cover_photo.created_time':'cover_photo_created_time','cover_photo.name':'cover_photo_name','cover_photo.id':'cover_photo_id','from.name':'from_name','likes.data':'likes_data','likes.paging.cursors.before':'likes_paging_cursors_before','likes.paging.cursors.after':'likes_paging_cursors_after','picture.data.is_silhouette':'picture_data_is_silhouette','picture.data.url':'picture_data_url','place.name':'place_name','place.location.latitude':'place_location_latitude','place.location.longitude':'place_location_longitude','place.id':'place_id'}, inplace = True)
            albums_dataframe['id_fb_user']=id_fb_user

            # Process columns to set as integer
            albums_columns_int = ['count','cover_photo_id','place_id']

            # Process columns to set as string
            albums_columns_str = ['id_media','media_type','link','name','privacy','type','cover_photo_name','from_name','likes_data','likes_paging_cursors_before','likes_paging_cursors_after','picture_data_url','description','backdated_time_granularity','location','place_name','place_location_latitude','place_location_longitude']
            
            #function call frame_validate to add keys wich doesn't exist 
            albums_dataframe = frame_validate(album_column_order, albums_dataframe)

            albums_dataframe['cover_photo_created_time'] = pd.to_datetime(albums_dataframe['cover_photo_created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            albums_dataframe['created_time'] = pd.to_datetime(albums_dataframe['created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            albums_dataframe['updated_time'] = pd.to_datetime(albums_dataframe['updated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            albums_dataframe['backdated_time'] = pd.to_datetime(albums_dataframe['backdated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            albums_dataframe['process_timestamp'] = pd.to_datetime(albums_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')

            # Transform column types
            albums_dataframe[albums_columns_str] = albums_dataframe[albums_columns_str].astype(str)
            albums_dataframe[albums_columns_int] = albums_dataframe[albums_columns_int].fillna(0).astype(int)
            
            # Upload data to BigQuery
            bigquery_config_albums_info(albums_dataframe[album_column_order], 'albums_info', fb_albums_info_table)
        
        #Photo_Albums info
    
        photo_albums_datainfo = []
        
        for photo_album_id in albums_photo_id_list:
            
            # Build request URL with proper parameters
            photo_album_url_info=f'https://graph.facebook.com/v15.0/{photo_album_id}'+'?fields=album,alt_text,alt_text_custom,backdated_time,backdated_time_granularity,can_backdate,can_delete,can_tag,created_time,event,from,height,icon,id,images,link,name,name_tags,page_story_id,place,target,updated_time,webp_images,width,comments.limit(0),likes.limit(0),picture'+f'&access_token={token}'
            
            # Try to send the request to the API and Return JSON object of the response
            photo_album_data = get_info_from_url(photo_album_url_info, 'photo_albums_info')
        
            photo_exception_keys = photo_album_data.keys()
            
            #Replace some columns with values
            if 'id' in photo_exception_keys:
                #photo_album_data['id'] = str(id_fb_user)+"_"+str(photo_album_data['id'])
                photo_album_data['id'] = str(photo_album_data['id'])

            #Clean name
            if 'name' in photo_exception_keys:    
                photo_album_data['name'] = photo_album_data['name'].replace('\n', ' ').replace('  ', ' ')

            # Clean timestamp objects from the response
            #if 'created_time' in photo_exception_keys:   
            #    photo_album_data['created_time'] = photo_album_data['created_time'][:-5]
            #    photo_album_clean_timestamp = datetime.strptime(photo_album_data['created_time'], "%Y-%m-%dT%H:%M:%S")
            #    photo_album_data['created_time'] = str(datetime.strptime(str(photo_album_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            #if 'updated_time' in photo_exception_keys:
            #    photo_album_data['updated_time'] = photo_album_data['updated_time'][:-5]
            #    photo_album_up_clean_timestamp = datetime.strptime(photo_album_data['updated_time'], "%Y-%m-%dT%H:%M:%S")
            #    photo_album_data['updated_time'] = str(datetime.strptime(str(photo_album_up_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            #if 'album' in photo_exception_keys:
            #    photo_album_data['album']['created_time'] = photo_album_data['album']['created_time'][:-5]
            #    photo_album_al_clean_timestamp = datetime.strptime(photo_album_data['album']['created_time'], "%Y-%m-%dT%H:%M:%S")
            #    photo_album_data['album']['created_time'] = str(datetime.strptime(str(photo_album_al_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'        

            
            photo_albums_datainfo.append(photo_album_data)

        
        #Create dataframe
        
        # Column order to match database table
        photo_column_order = ['id_fb_user','id_media','process_timestamp','media_type','created_time','alt_text','can_backdate','can_delete','can_tag','height','icon','images','link','name','page_story_id','updated_time','webp_images','width','picture','album_created_time','album_name','album_id','from_name','alt_text_custom','name_tags']

        # Transform json object to a dataframe
        photo_albums_dataframe = pd.json_normalize(photo_albums_datainfo)
        
        if (len(photo_albums_dataframe)) > 0:
            # Add process timestamp and type to dataframe
            photo_albums_dataframe.insert(0, 'media_type','photo')
            photo_albums_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
            
            # Rename columns
            photo_albums_dataframe.rename(columns = {'from.id':'id_fb_user','id':'id_media','album.created_time':'album_created_time','album.name':'album_name','album.id':'album_id','from.name':'from_name'}, inplace = True)
            photo_albums_dataframe=frame_validate(photo_column_order, photo_albums_dataframe)

            photo_albums_dataframe['id_fb_user'] = id_fb_user
            # Process columns to set as integer
            photo_albums_columns_int = ['id_fb_user','height','width','album_id']

            # Process columns to set as string
            photo_albums_columns_str = ['id_media','media_type','alt_text','icon','images','link','name','page_story_id','webp_images','picture','album_name','from_name','alt_text_custom','name_tags']

            #function call frame_validate to add keys wich doesn't exist 
            photo_albums_dataframe = frame_validate(photo_column_order, photo_albums_dataframe)

            # Transform column types
            photo_albums_dataframe[photo_albums_columns_str] = photo_albums_dataframe[photo_albums_columns_str].astype(str)
            photo_albums_dataframe[photo_albums_columns_int] = photo_albums_dataframe[photo_albums_columns_int].fillna(0).astype(int)
            photo_albums_dataframe['created_time'] = pd.to_datetime(photo_albums_dataframe['created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            photo_albums_dataframe['updated_time'] = pd.to_datetime(photo_albums_dataframe['updated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            photo_albums_dataframe['album_created_time'] = pd.to_datetime(photo_albums_dataframe['album_created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            photo_albums_dataframe['process_timestamp'] = pd.to_datetime(photo_albums_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')
            
            #try csv with columns_order
            photo_albums_dataframe = photo_albums_dataframe[photo_column_order]
            photo_albums_dataframe.to_csv('prueba_photo.csv')
  
            bigquery_config_photo_albums_info(photo_albums_dataframe,'photo_albums_info',fb_photo_albums_info_table)  

    elif process_type in ('weekly', 'monthly', 'daily-week'):
        
        # photo_albums info
        photo_albums_datainfo = []
        
        for photo_album_id in albums_photo_id_list:
            
            # Build request URL with proper parameters
            photo_album_url_info = f'https://graph.facebook.com/v15.0/{photo_album_id}'+'?fields=album,alt_text,alt_text_custom,backdated_time,backdated_time_granularity,can_backdate,can_delete,can_tag,created_time,event,from,height,icon,id,images,link,name,name_tags,page_story_id,place,target,updated_time,webp_images,width,comments.limit(0),likes.limit(0),picture'+f'&access_token={token}'
            
            # Try to send the request to the API and Return JSON object of the response
            photo_album_data = get_info_from_url(photo_album_url_info,'photo_albums_info')
        
            photo_exception_keys = photo_album_data.keys()
            
            #Replace some columns with values
            if 'id' in photo_exception_keys:
                #photo_album_data['id']=str(id_fb_user)+"_"+str(photo_album_data['id'])
                photo_album_data['id'] = str(photo_album_data['id'])

            #Clean name
            if 'name' in photo_exception_keys:    
                photo_album_data['name'] = photo_album_data['name'].replace('\n', ' ').replace('  ', ' ')

            # Clean timestamp objects from the response
            #if 'created_time' in photo_exception_keys:   
            #    photo_album_data['created_time'] = photo_album_data['created_time'][:-5]
            #    photo_album_clean_timestamp = datetime.strptime(photo_album_data['created_time'], "%Y-%m-%dT%H:%M:%S")
            #    photo_album_data['created_time'] = str(datetime.strptime(str(photo_album_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            #if 'updated_time' in photo_exception_keys:
            #    photo_album_data['updated_time'] = photo_album_data['updated_time'][:-5]
            #    photo_album_up_clean_timestamp = datetime.strptime(photo_album_data['updated_time'], "%Y-%m-%dT%H:%M:%S")
            #    photo_album_data['updated_time'] = str(datetime.strptime(str(photo_album_up_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            #if 'album' in photo_exception_keys:
            #    photo_album_data['album']['created_time'] = photo_album_data['album']['created_time'][:-5]
            #    photo_album_al_clean_timestamp = datetime.strptime(photo_album_data['album']['created_time'], "%Y-%m-%dT%H:%M:%S")
            #    photo_album_data['album']['created_time'] = str(datetime.strptime(str(photo_album_al_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'     

            
            photo_albums_datainfo.append(photo_album_data)

        # Column order to match database table
        photo_column_order = ['id_fb_user','id_media','process_timestamp','media_type','created_time','alt_text','can_backdate','can_delete','can_tag','height','icon','images','link','name','page_story_id','updated_time','webp_images','width','picture','album_created_time','album_name','album_id','from_name','alt_text_custom','name_tags']

        # Transform json object to a dataframe
        photo_albums_dataframe = pd.json_normalize(photo_albums_datainfo)
        

        if (len(photo_albums_dataframe)) > 0:
            # Add process timestamp and type to dataframe
            photo_albums_dataframe.insert(0, 'media_type','photo')
            photo_albums_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
            
            # Rename columns
            photo_albums_dataframe.rename(columns = {'from.id':'id_fb_user','id':'id_media','album.created_time':'album_created_time','album.name':'album_name','album.id':'album_id','from.name':'from_name'}, inplace = True)
            photo_albums_dataframe=frame_validate(photo_column_order, photo_albums_dataframe)

            photo_albums_dataframe['id_fb_user'] = id_fb_user
            # Process columns to set as integer
            photo_albums_columns_int = ['id_fb_user','height','width','album_id']

            # Process columns to set as string
            photo_albums_columns_str = ['id_media','media_type','alt_text','icon','images','link','name','page_story_id','webp_images','picture','album_name','from_name','alt_text_custom','name_tags']

            #function call frame_validate to add keys wich doesn't exist 
            photo_albums_dataframe = frame_validate(photo_column_order, photo_albums_dataframe)

            # Transform column types
            photo_albums_dataframe[photo_albums_columns_str] = photo_albums_dataframe[photo_albums_columns_str].astype(str)
            photo_albums_dataframe[photo_albums_columns_int] = photo_albums_dataframe[photo_albums_columns_int].fillna(0).astype(int)
            photo_albums_dataframe['created_time'] = pd.to_datetime(photo_albums_dataframe['created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            photo_albums_dataframe['updated_time'] = pd.to_datetime(photo_albums_dataframe['updated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            photo_albums_dataframe['album_created_time'] = pd.to_datetime(photo_albums_dataframe['album_created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            photo_albums_dataframe['process_timestamp'] = pd.to_datetime(photo_albums_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')
            
            # Upload data to BigQuery
            bigquery_config_photo_albums_info(photo_albums_dataframe[photo_column_order],'photo_albums_info',fb_photo_albums_info_table)  


def bigquery_config_video_lists_info(dframe,tabla,fb_video_lists_info_table):
    client = bigquery.Client()
    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
            #Specify the type of columns whose type cannot be auto-detected. For
            # example the "page_fans_locale" column uses pandas dtype "object", so its
            # data type is ambiguous.          
            bigquery.SchemaField(name ='id_fb_user',field_type ='INTEGER'),
            bigquery.SchemaField(name ='id_media',field_type ='STRING'),
            bigquery.SchemaField(name ='process_timestamp',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='media_type',field_type ='STRING'),
            bigquery.SchemaField(name ='creation_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='last_modified',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='thumbnail',field_type ='STRING'),
            bigquery.SchemaField(name ='title',field_type ='STRING'),
            bigquery.SchemaField(name ='videos_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='description',field_type ='STRING'),
            bigquery.SchemaField(name ='owner_name',field_type ='STRING'),
            bigquery.SchemaField(name ='videos_data',field_type ='STRING'),
            bigquery.SchemaField(name ='videos_paging_cursors_before',field_type ='STRING'),
            bigquery.SchemaField(name ='videos_paging_cursors_after',field_type ='STRING'),
            bigquery.SchemaField(name ='videos_paging_next',field_type ='STRING')

        ]
    )
    upload_bigquery_data(dframe,tabla,fb_video_lists_info_table,job_config)
    #print(f"{tabla} data uploaded successfully. CHECK")

def bigquery_config_video_info(dframe,tabla,fb_video_info_table):
    client = bigquery.Client()
    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
            #Specify the type of columns whose type cannot be auto-detected. For
            # example the "page_fans_locale" column uses pandas dtype "object", so its
            # data type is ambiguous.          
            bigquery.SchemaField(name ='id_fb_user',field_type ='INTEGER'),
            bigquery.SchemaField(name ='id_media',field_type ='STRING'),
            bigquery.SchemaField(name ='process_timestamp',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='media_type',field_type ='STRING'),
            bigquery.SchemaField(name ='created_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='ad_breaks',field_type ='STRING'),
            bigquery.SchemaField(name ='content_category',field_type ='STRING'),
            bigquery.SchemaField(name ='description',field_type ='STRING'),
            bigquery.SchemaField(name ='embed_html',field_type ='STRING'),
            bigquery.SchemaField(name ='embeddable',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='format',field_type ='STRING'),
            bigquery.SchemaField(name ='icon',field_type ='STRING'),
            bigquery.SchemaField(name ='is_crosspost_video',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_crossposting_eligible',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_episode',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_instagram_eligible',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='is_reference_only',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='length',field_type ='FLOAT'),
            bigquery.SchemaField(name ='live_status',field_type ='STRING'),
            bigquery.SchemaField(name ='post_views',field_type ='INTEGER'),
            bigquery.SchemaField(name ='published',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='source',field_type ='STRING'),
            bigquery.SchemaField(name ='title',field_type ='STRING'),
            bigquery.SchemaField(name ='updated_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='views',field_type ='INTEGER'),
            bigquery.SchemaField(name ='permalink_url',field_type ='STRING'),
            bigquery.SchemaField(name ='picture',field_type ='STRING'),
            bigquery.SchemaField(name ='from_name',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_allow',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_deny',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_description',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_friends',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_networks',field_type ='STRING'),
            bigquery.SchemaField(name ='privacy_value',field_type ='STRING'),
            bigquery.SchemaField(name ='status_video_status',field_type ='STRING'),
            bigquery.SchemaField(name ='status_uploading_phase_status',field_type ='STRING'),
            bigquery.SchemaField(name ='status_processing_phase_status',field_type ='STRING'),
            bigquery.SchemaField(name ='status_publishing_phase_status',field_type ='STRING'),
            bigquery.SchemaField(name ='status_publishing_phase_publish_status',field_type ='STRING'),
            bigquery.SchemaField(name ='status_publishing_phase_publish_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='comments_data',field_type ='STRING'),
            bigquery.SchemaField(name ='comments_summary_total_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='likes_data',field_type ='STRING'),
            bigquery.SchemaField(name ='likes_summary_total_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='thumbnails_data',field_type ='STRING'),
            bigquery.SchemaField(name ='custom_labels',field_type ='STRING'),
            bigquery.SchemaField(name ='captions_data',field_type ='STRING'),
            bigquery.SchemaField(name ='universal_video_id',field_type ='STRING'),
            bigquery.SchemaField(name ='backdated_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='backdated_time_granularity',field_type ='STRING')

        ]
    )
    upload_bigquery_data(dframe,tabla,fb_video_info_table,job_config)
    #print(f"{tabla} data uploaded successfully. CHECK")

def bigquery_config_live_vids_info(dframe,tabla,fb_live_video_info_table):
    client = bigquery.Client() #redundante
    # Create a job config to upload data
    job_config = bigquery.LoadJobConfig(schema = [
            #Specify the type of columns whose type cannot be auto-detected. For
            # example the "page_fans_locale" column uses pandas dtype "object", so its
            # data type is ambiguous.          
            bigquery.SchemaField(name ='id_fb_user',field_type ='INTEGER'),
            bigquery.SchemaField(name ='id_media',field_type ='STRING'),
            bigquery.SchemaField(name ='process_timestamp',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='media_type',field_type ='STRING'),
            bigquery.SchemaField(name ='creation_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='ad_break_failure_reason',field_type ='STRING'),
            bigquery.SchemaField(name ='broadcast_start_time',field_type ='TIMESTAMP'),
            bigquery.SchemaField(name ='dash_ingest_url',field_type ='STRING'),
            bigquery.SchemaField(name ='dash_preview_url',field_type ='STRING'),
            bigquery.SchemaField(name ='description',field_type ='STRING'),
            bigquery.SchemaField(name ='embed_html',field_type ='STRING'),
            bigquery.SchemaField(name ='ingest_streams',field_type ='STRING'),
            bigquery.SchemaField(name ='is_reference_only',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='overlay_url',field_type ='STRING'),
            bigquery.SchemaField(name ='live_views',field_type ='INTEGER'),
            bigquery.SchemaField(name ='seconds_left',field_type ='FLOAT'),
            bigquery.SchemaField(name ='secure_stream_url',field_type ='STRING'),
            bigquery.SchemaField(name ='status',field_type ='STRING'),
            bigquery.SchemaField(name ='stream_url',field_type ='STRING'),
            bigquery.SchemaField(name ='title',field_type ='STRING'),
            bigquery.SchemaField(name ='permalink_url',field_type ='STRING'),
            bigquery.SchemaField(name ='ad_break_config_is_eligible_to_onboard',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='ad_break_config_guide_url',field_type ='STRING'),
            bigquery.SchemaField(name ='ad_break_config_onboarding_url',field_type ='STRING'),
            bigquery.SchemaField(name ='ad_break_config_is_enabled',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='ad_break_config_default_ad_break_duration',field_type ='INTEGER'),
            bigquery.SchemaField(name ='ad_break_config_failure_reason_polling_interval',field_type ='INTEGER'),
            bigquery.SchemaField(name ='ad_break_config_preparing_duration',field_type ='INTEGER'),
            bigquery.SchemaField(name ='ad_break_config_viewer_count_threshold',field_type ='INTEGER'),
            bigquery.SchemaField(name ='ad_break_config_time_between_ad_breaks_secs',field_type ='INTEGER'),
            bigquery.SchemaField(name ='ad_break_config_first_break_eligible_secs',field_type ='INTEGER'),
            bigquery.SchemaField(name ='from_name',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_streaming_protocol',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_codec',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_max_bitrate',field_type ='INTEGER'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_max_width',field_type ='INTEGER'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_max_height',field_type ='INTEGER'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_max_framerate',field_type ='INTEGER'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_profile',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_level',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_scan_mode',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_rate_control_mode',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_gop_size_in_seconds',field_type ='INTEGER'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_gop_type',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_video_codec_settings_gop_closed',field_type ='BOOLEAN'),
            bigquery.SchemaField(name ='recommended_encoder_settings_audio_codec_settings_codec',field_type ='STRING'),
            bigquery.SchemaField(name ='recommended_encoder_settings_audio_codec_settings_bitrate',field_type ='INTEGER'),
            bigquery.SchemaField(name ='recommended_encoder_settings_audio_codec_settings_channels',field_type ='INTEGER'),
            bigquery.SchemaField(name ='recommended_encoder_settings_audio_codec_settings_samplerate',field_type ='INTEGER'),
            bigquery.SchemaField(name ='video_live_id',field_type ='STRING'),
            bigquery.SchemaField(name ='comments_data',field_type ='STRING'),
            bigquery.SchemaField(name ='comments_summary_total_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='reactions_data',field_type ='STRING'),
            bigquery.SchemaField(name ='reactions_summary_total_count',field_type ='INTEGER'),
            bigquery.SchemaField(name ='errors_data',field_type ='STRING'),
            bigquery.SchemaField(name ='crosspost_shared_pages_data',field_type ='STRING'),
            bigquery.SchemaField(name ='crossposted_broadcasts_data',field_type ='STRING')

        ]
    )
    upload_bigquery_data(dframe,tabla,fb_live_video_info_table,job_config)#renombrar "tabla" como nombre de funcion
    #print(f"{tabla} data uploaded successfully. CHECK")

def video_info(process_type,id_fb_user, since, until, token, video_lists_id_list, vids_id_list, live_vids_id_list, reels_vids_id_list, fb_video_lists_info_table, fb_video_info_table, fb_live_video_info_table):
    
    """
    Get video_list info
    """
    if process_type =='daily':
        # Instantiate an empty list to store video lists info
        video_list_datainfo = []
        
        # Iterate over each element in the video_list_id_list
        for video_list_id in video_lists_id_list:
        
            # Build request URL with proper parameters
            video_list_url_info = f'https://graph.facebook.com/v15.0/{video_list_id}'+'?fields=creation_time,description,id,last_modified,owner,season_number,thumbnail,title,videos_count,videos'+f'&access_token={token}'
            
            # Try to send the request to the API and Return JSON object of the response
            video_list_data = get_info_from_url(video_list_url_info,'video_list_info')
        
            # Get the keys from the video_list dictionary
            video_list_exception_keys = video_list_data.keys()
            
            # Replace some columns with values
            if 'id' in video_list_exception_keys:
                #video_list_data['id']=str(id_fb_user)+"_"+str(video_list_data['id'])
                video_list_data['id'] = str(video_list_data['id'])
                
            # Clean name
            if 'description' in video_list_exception_keys:              
                video_list_data['description'] = video_list_data['description'].replace('\n', ' ').replace('  ', ' ')
                
            # Clean timestamp objects from the response
            #if 'creation_time' in video_list_exception_keys:            
            #    video_list_data['creation_time'] = video_list_data['creation_time'][:-5]
            #    video_list_clean_timestamp = datetime.strptime(video_list_data['creation_time'], "%Y-%m-%dT%H:%M:%S")
            #    video_list_data['creation_time'] = str(datetime.strptime(str(video_list_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
            # Clean last_modified timestamp
            #if 'last_modified' in video_list_exception_keys:            
            #    video_list_data['last_modified'] = video_list_data['last_modified'][:-5]
            #    video_list_last_clean_timestamp = datetime.strptime(video_list_data['last_modified'], "%Y-%m-%dT%H:%M:%S")
            #    video_list_data['last_modified'] = str(datetime.strptime(str(video_list_last_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
                
            # Append the video_list_data to video_list data info list    
            video_list_datainfo.append(video_list_data)
        
        # Column order to match database table
        video_list_column_order = ['id_fb_user','id_media','process_timestamp','media_type','creation_time','last_modified',
                                'thumbnail','title','videos_count','description','owner_name','videos_data',
                                'videos_paging_cursors_before','videos_paging_cursors_after','videos_paging_next']


        # Transform json object to a dataframe
        video_list_dataframe = pd.json_normalize(video_list_datainfo)
        
        # If there are elements in the dataframe
        if len(video_list_dataframe) > 0:
            
            # Add process timestamp and type to dataframe
            video_list_dataframe.insert(0, 'media_type', 'video list')
            video_list_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
        
            # Rename columns
            video_list_dataframe.rename(columns = {'owner.id':'id_fb_user','id':'id_media','owner.name':'owner_name','videos.data':'videos_data','videos.paging.cursors.before':'videos_paging_cursors_before','videos.paging.cursors.after':'videos_paging_cursors_after','videos.paging.next':'videos_paging_next'}, inplace = True)
            video_list_dataframe['id_fb_user'] = id_fb_user
            
            # Process columns to set as integer
            video_list_int = ['videos_count']

            # Process columns to set as string
            video_list_columns_str = ['id_media','media_type','thumbnail','title','description','owner_name','videos_data','videos_paging_cursors_before','videos_paging_cursors_after','videos_paging_next']

            # Transform column types
            video_list_dataframe[video_list_columns_str] = video_list_dataframe[video_list_columns_str].astype(str)
            video_list_dataframe[video_list_int] = video_list_dataframe[video_list_int].astype(int)
            video_list_dataframe['creation_time']=pd.to_datetime(video_list_dataframe['creation_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            video_list_dataframe['last_modified']=pd.to_datetime(video_list_dataframe['last_modified'], format = '%Y-%m-%dT%H:%M:%S+0000')
            video_list_dataframe['process_timestamp'] = pd.to_datetime(video_list_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')

            #video_list_dataframe=video_list_dataframe[video_list_column_order]
            bigquery_config_video_lists_info(video_list_dataframe[video_list_column_order],'video_lists_info',fb_video_lists_info_table)
            

        """
        Get video general info
        """
        
        # Instantiate an empty list to store video lists info
        video_datainfo = []
        
        # Iterate over each element in the video_list_id_list
        for video_id in vids_id_list:
            
            # Build request URL with proper parameters
            video_url_info = f'https://graph.facebook.com/v15.0/{video_id}'+'?fields=ad_breaks,backdated_time,backdated_time_granularity,content_category,content_tags,created_time,custom_labels,description,embed_html,embeddable,event,format,from,icon,id,is_crosspost_video,is_crossposting_eligible,is_episode,is_instagram_eligible,is_reference_only,length,live_status,music_video_copyright,place,post_views,premiere_living_room_status,privacy,published,scheduled_publish_time,source,status,title,universal_video_id,updated_time,views,captions{create_time,is_auto_generated,is_default,locale,locale_name,uri},comments.limit(0).summary(total_count),likes.limit(0).summary(total_count),permalink_url,picture,poll_settings{id,video_poll_www_placement,was_live_voting_enabled},polls{id,question,show_results,status,poll_options},tags,thumbnails{height,id,is_preferred,name,scale,uri,width}'+f'&access_token={token}'
            
            # Try to send the request to the API and Return JSON object of the response
            video_data = get_info_from_url(video_url_info, 'video_info')
            
            # Get the keys from the video_list dictionary
            video_exception_keys = video_data.keys()
            
            #Replace some columns with values
            if 'id' in video_exception_keys:
                #video_data['id']=str(id_fb_user)+"_"+str(video_data['id'])
                video_data['id'] = str(video_data['id'])
            
            # Clean name
            if 'description' in video_exception_keys:
                video_data['description'] = video_data['description'].replace('\n', ' ').replace('  ', ' ')
                
            # Clean timestamp objects from the response
            #if 'created_time' in video_exception_keys:            
            #    video_data['created_time'] = video_data['created_time'][:-5]
            #    video_clean_timestamp = datetime.strptime(video_data['created_time'], "%Y-%m-%dT%H:%M:%S")
            #    video_data['created_time'] = str(datetime.strptime(str(video_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
            # Clean updated time field
            #if 'updated_time' in video_exception_keys:
            #    video_data['updated_time'] = video_data['updated_time'][:-5]
            #    video_up_clean_timestamp = datetime.strptime(video_data['updated_time'], "%Y-%m-%dT%H:%M:%S")
            #    video_data['updated_time'] = str(datetime.strptime(str(video_up_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
                
            # Clean status field
            #if 'status' in video_exception_keys and 'publishing_phase' in video_exception_keys and 'publish_time' in video_exception_keys:
            #    video_data['status']['publishing_phase']['publish_time'] = video_data['status']['publishing_phase']['publish_time'][:-5]
            #    status_video_clean_timestamp = datetime.strptime(video_data['status']['publishing_phase']['publish_time'], "%Y-%m-%dT%H:%M:%S")
            #    video_data['status']['publishing_phase']['publish_time'] = str(datetime.strptime(str(status_video_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
            # Clean backdated_time field
            #if 'backdated_time' in video_exception_keys:
            #    video_data['backdated_time'] = video_data['backdated_time'][:-5]
            #    video_back_clean_timestamp = datetime.strptime(video_data['backdated_time'], "%Y-%m-%dT%H:%M:%S")
            #    video_data['backdated_time'] = str(datetime.strptime(str(video_back_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
                
            # Append the video_data to video_data info list               
            video_datainfo.append(video_data)
        
        # Transform json object to a dataframe
        video_dataframe = pd.json_normalize(video_datainfo)
        
        # If there are elements in the dataframe
        if len(video_dataframe) > 0:

            # Add process timestamp and type to dataframe
            video_dataframe.insert(0,'media_type','video')
            video_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
        
        # Rename columns
        video_dataframe.rename(columns = {'from.id':'id_fb_user','id':'id_media','from.name':'from_name','privacy.allow':'privacy_allow','privacy.deny':'privacy_deny','privacy.description':'privacy_description','privacy.friends':'privacy_friends','privacy.networks':'privacy_networks','privacy.value':'privacy_value','status.video_status':'status_video_status','status.uploading_phase.status':'status_uploading_phase_status','status.processing_phase.status':'status_processing_phase_status','status.publishing_phase.status':'status_publishing_phase_status','status.publishing_phase.publish_status':'status_publishing_phase_publish_status','status.publishing_phase.publish_time':'status_publishing_phase_publish_time','comments.data':'comments_data','comments.summary.total_count':'comments_summary_total_count','likes.data':'likes_data','likes.summary.total_count':'likes_summary_total_count','thumbnails.data':'thumbnails_data','captions.data':'captions_data'}, inplace = True)
        video_dataframe['id_fb_user'] = id_fb_user

        """
        Get reels info
        """
        
        reels_vids_datainfo=[]

        for reels_vids_id in reels_vids_id_list:
                
            # Build request URL with proper parameters
            reels_vids_url_info = f'https://graph.facebook.com/v15.0/{reels_vids_id}'+'?fields=ad_breaks,backdated_time,backdated_time_granularity,content_category,content_tags,created_time,custom_labels,description,embed_html,embeddable,event,format,from,icon,id,is_crosspost_video,is_crossposting_eligible,is_episode,is_instagram_eligible,is_reference_only,length,live_status,music_video_copyright,place,post_views,premiere_living_room_status,privacy,published,scheduled_publish_time,source,status,title,universal_video_id,updated_time,views,captions{create_time,is_auto_generated,is_default,locale,locale_name,uri},comments.limit(0).summary(total_count),likes.limit(0).summary(total_count),permalink_url,picture,poll_settings{id,video_poll_www_placement,was_live_voting_enabled},polls{id,question,show_results,status,poll_options},tags,thumbnails{height,id,is_preferred,name,scale,uri,width}'+f'&access_token={token}'
            
            # Try to send the request to the API and Return JSON object of the response
            reels_vids_data = get_info_from_url(reels_vids_url_info,'reels_info')
                    
            reels_exception_keys = reels_vids_data.keys()
            
            #Replace some columns with values
            if 'id' in reels_exception_keys:
                #reels_vids_data['id']=str(id_fb_user)+"_"+str(reels_vids_data['id'])
                reels_vids_data['id'] = str(reels_vids_data['id'])
            
            #Clean name
            if 'description' in reels_exception_keys:            
                reels_vids_data['description'] = reels_vids_data['description'].replace('\n', ' ').replace('  ', ' ')
                
            # Clean the created_time from the response
            #if 'created_time' in reels_exception_keys:            
            #    reels_vids_data['created_time'] = reels_vids_data['created_time'][:-5]
            #    reels_vids_clean_timestamp = datetime.strptime(reels_vids_data['created_time'], "%Y-%m-%dT%H:%M:%S")
            #    reels_vids_data['created_time'] = str(datetime.strptime(str(reels_vids_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            #if 'updated_time' in reels_exception_keys:            
            #    reels_vids_data['updated_time'] = reels_vids_data['updated_time'][:-5]
            #    reels_vids_up_clean_timestamp = datetime.strptime(reels_vids_data['updated_time'], "%Y-%m-%dT%H:%M:%S")
            #    video_data['updated_time'] = str(datetime.strptime(str(reels_vids_up_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            #if 'status' in reels_exception_keys:            
            #    reels_vids_data['status']['publishing_phase']['publish_time'] = reels_vids_data['status']['publishing_phase']['publish_time'][:-5]
            #    reels_vids_status_clean_timestamp = datetime.strptime(reels_vids_data['status']['publishing_phase']['publish_time'], "%Y-%m-%dT%H:%M:%S")
            #    reels_vids_data['status']['publishing_phase']['publish_time'] = str(datetime.strptime(str(reels_vids_status_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
            #if 'backdated_time' in reels_exception_keys:
            #    reels_vids_data['backdated_time'] = reels_vids_data['backdated_time'][:-5]
            #    reels_back_clean_timestamp = datetime.strptime(video_data['backdated_time'], "%Y-%m-%dT%H:%M:%S")
            #    reels_vids_data['backdated_time'] = str(datetime.strptime(str(reels_back_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            reels_vids_datainfo.append(reels_vids_data)

            """
            if (reels_vids_data['status']['publishing_phase']['status'] == 'complete'):
                reels_vids_datainfo.append(reels_vids_data)
            else:
                continue
            """

        # Transform json dict to a dataframe
        reels_vids_dataframe = pd.json_normalize(reels_vids_datainfo)
        
        # Add process timestamp and type to dataframe
        if len(reels_vids_dataframe)>0:
            reels_vids_dataframe.insert(0, 'media_type', 'video reel')
            reels_vids_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
        
        # Rename columns
        reels_vids_dataframe.rename(columns = {'from.id':'id_fb_user','id':'id_media','from.name':'from_name','privacy.allow':'privacy_allow','privacy.deny':'privacy_deny','privacy.description':'privacy_description','privacy.friends':'privacy_friends','privacy.networks':'privacy_networks','privacy.value':'privacy_value','status.video_status':'status_video_status','status.uploading_phase.status':'status_uploading_phase_status','status.processing_phase.status':'status_processing_phase_status','status.publishing_phase.status':'status_publishing_phase_status','status.publishing_phase.publish_status':'status_publishing_phase_publish_status','status.publishing_phase.publish_time':'status_publishing_phase_publish_time','comments.data':'comments_data','comments.summary.total_count':'comments_summary_total_count','likes.data':'likes_data','likes.summary.total_count':'likes_summary_total_count','thumbnails.data':'thumbnails_data','captions.data':'captions_data'}, inplace = True)
        reels_vids_dataframe['id_fb_user'] = id_fb_user
        
        # Column order to match database table
        video_column_order = ['id_fb_user','id_media','process_timestamp','media_type','created_time','ad_breaks','content_category','description','embed_html','embeddable','format','icon','is_crosspost_video','is_crossposting_eligible','is_episode','is_instagram_eligible','is_reference_only','length','live_status','post_views','published','source','title','updated_time','views','permalink_url','picture','from_name','privacy_allow','privacy_deny','privacy_description','privacy_friends','privacy_networks','privacy_value','status_video_status','status_uploading_phase_status','status_processing_phase_status','status_publishing_phase_status','status_publishing_phase_publish_status','status_publishing_phase_publish_time','comments_data','comments_summary_total_count','likes_data','likes_summary_total_count','thumbnails_data','custom_labels','captions_data','universal_video_id','backdated_time','backdated_time_granularity']


        #Concat video and video_reels dataframes
        unique_vids_dataframe = pd.concat([video_dataframe, reels_vids_dataframe])

        if len(unique_vids_dataframe) > 0:
            unique_vids_dataframe = frame_validate(video_column_order, unique_vids_dataframe)

            # Process columns to set as integer
            unique_vids_columns_int = ['post_views','views','comments_summary_total_count','likes_summary_total_count']

            # Process columns to set as string
            unique_vids_columns_str = ['id_media','media_type','ad_breaks','content_category','description','embed_html','format','icon','live_status','source','title','permalink_url','picture','from_name','privacy_allow','privacy_deny','privacy_description','privacy_friends','privacy_networks','privacy_value','status_video_status','status_uploading_phase_status','status_processing_phase_status','status_publishing_phase_status','status_publishing_phase_publish_status','comments_data','likes_data','thumbnails_data','custom_labels','captions_data','universal_video_id']

            # Transform column types
            unique_vids_dataframe[unique_vids_columns_str] = unique_vids_dataframe[unique_vids_columns_str].astype(str)
            unique_vids_dataframe[unique_vids_columns_int] = unique_vids_dataframe[unique_vids_columns_int].fillna(0).astype(int)
            unique_vids_dataframe['created_time'] = pd.to_datetime(unique_vids_dataframe['created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            unique_vids_dataframe['updated_time'] = pd.to_datetime(unique_vids_dataframe['updated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            unique_vids_dataframe['status_publishing_phase_publish_time']=pd.to_datetime(unique_vids_dataframe['status_publishing_phase_publish_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            unique_vids_dataframe['backdated_time'] = pd.to_datetime(unique_vids_dataframe['backdated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            unique_vids_dataframe['process_timestamp'] = pd.to_datetime(unique_vids_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')

            # Upload data to BigQuery
            bigquery_config_video_info(unique_vids_dataframe[video_column_order], 'video_info', fb_video_info_table)

        #live video info
        
        live_vids_datainfo = []
        live_videos_id_list_for_insights = []
        
        for live_vids_id in live_vids_id_list:
            
            # Build request URL with proper parameters
            live_vids_url_info = f'https://graph.facebook.com/v15.0/{live_vids_id}'+'?fields=broadcast_start_time,copyright,creation_time,dash_ingest_url,dash_preview_url,description,embed_html,from,id,ingest_streams,is_reference_only,is_manual_mode,overlay_url,live_views,planned_start_time,seconds_left,recommended_encoder_settings,secure_stream_url,status,stream_url,title,targeting,video,blocked_users.limit(0),comments.limit(0).summary(total_count),reactions.limit(0).summary(total_count),crosspost_shared_pages,crossposted_broadcasts,errors{creation_time,error_code,error_message,error_type},permalink_url,polls'+f'&access_token={token}'
            
            # Try to send the request to the API and Return JSON object of the response
            live_vids_data = get_info_from_url(live_vids_url_info,'live_vids_info')
            
            # Get the columns from live_vids_data dict
            live_vids_exception_keys = live_vids_data.keys()
            
            # Replace some columns with values
            if 'id' in live_vids_exception_keys:
            #    print(str(live_vids_data['id']))
            #    print(str(live_vids_data['id']))
            #    print(str(live_vids_id))
                live_vids_data['id'] = str(live_vids_data['id'])
            
            # Clean name
            if 'description' in live_vids_exception_keys:
                live_vids_data['description'] = live_vids_data['description'].replace('\n', ' ').replace('  ', ' ')
                
            # Clean the timestamp type objects from the response
            #if 'creation_time' in live_vids_exception_keys:            
            #    live_vids_data['creation_time'] = live_vids_data['creation_time'][:-5]
            #    live_vids_clean_timestamp = datetime.strptime(live_vids_data['creation_time'], "%Y-%m-%dT%H:%M:%S")
            #    live_vids_data['creation_time'] = str(datetime.strptime(str(live_vids_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

            #if 'broadcast_start_time' in live_vids_exception_keys:  
            #    live_vids_data['broadcast_start_time'] = live_vids_data['broadcast_start_time'][:-5]
            #    live_vids_broad_clean_timestamp = datetime.strptime(live_vids_data['broadcast_start_time'], "%Y-%m-%dT%H:%M:%S")
            #    live_vids_data['broadcast_start_time'] = str(datetime.strptime(str(live_vids_broad_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
            
            
            live_videos_id_list_for_insights.append(live_vids_data['video']['id'])      
            live_vids_datainfo.append(live_vids_data)
        
        #Create dataframe
        # Column order to match database table
        live_vids_column_order = ['id_fb_user','id_media','process_timestamp','media_type','creation_time','ad_break_failure_reason',
                                'broadcast_start_time','dash_ingest_url','dash_preview_url','description','embed_html',
                                'ingest_streams','is_reference_only','overlay_url','live_views','seconds_left','secure_stream_url',
                                'status','stream_url','title','permalink_url','ad_break_config_is_eligible_to_onboard',
                                'ad_break_config_guide_url','ad_break_config_onboarding_url','ad_break_config_is_enabled',
                                'ad_break_config_default_ad_break_duration','ad_break_config_failure_reason_polling_interval',
                                'ad_break_config_preparing_duration','ad_break_config_viewer_count_threshold',
                                'ad_break_config_time_between_ad_breaks_secs','ad_break_config_first_break_eligible_secs',
                                'from_name','recommended_encoder_settings_streaming_protocol',
                                'recommended_encoder_settings_video_codec_settings_codec',
                                'recommended_encoder_settings_video_codec_settings_max_bitrate',
                                'recommended_encoder_settings_video_codec_settings_max_width',
                                'recommended_encoder_settings_video_codec_settings_max_height',
                                'recommended_encoder_settings_video_codec_settings_max_framerate',
                                'recommended_encoder_settings_video_codec_settings_profile',
                                'recommended_encoder_settings_video_codec_settings_level',
                                'recommended_encoder_settings_video_codec_settings_scan_mode',
                                'recommended_encoder_settings_video_codec_settings_rate_control_mode',
                                'recommended_encoder_settings_video_codec_settings_gop_size_in_seconds',
                                'recommended_encoder_settings_video_codec_settings_gop_type',
                                'recommended_encoder_settings_video_codec_settings_gop_closed',
                                'recommended_encoder_settings_audio_codec_settings_codec',
                                'recommended_encoder_settings_audio_codec_settings_bitrate',
                                'recommended_encoder_settings_audio_codec_settings_channels',
                                'recommended_encoder_settings_audio_codec_settings_samplerate','video_live_id','comments_data',
                                'comments_summary_total_count','reactions_data','reactions_summary_total_count','errors_data',
                                'crosspost_shared_pages_data','crossposted_broadcasts_data']


        # Transform json object to a dataframe
        live_vids_dataframe = pd.json_normalize(live_vids_datainfo)
        
        # Add process timestamp and post_type to dataframe
        if len(live_vids_dataframe) > 0:

            live_vids_dataframe.insert(0, 'media_type', 'live_video')
            live_vids_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
        
            # Rename columns to match database
            live_vids_dataframe.rename(columns = {'id':'video_live_id', 'from.id':'id_fb_user','video.id':'id_media','ad_break_config.is_eligible_to_onboard':'ad_break_config_is_eligible_to_onboard','ad_break_config.guide_url':'ad_break_config_guide_url','ad_break_config.onboarding_url':'ad_break_config_onboarding_url','ad_break_config.is_enabled':'ad_break_config_is_enabled','ad_break_config.default_ad_break_duration':'ad_break_config_default_ad_break_duration','ad_break_config.failure_reason_polling_interval':'ad_break_config_failure_reason_polling_interval','ad_break_config.preparing_duration':'ad_break_config_preparing_duration','ad_break_config.viewer_count_threshold':'ad_break_config_viewer_count_threshold','ad_break_config.time_between_ad_breaks_secs':'ad_break_config_time_between_ad_breaks_secs','ad_break_config.first_break_eligible_secs':'ad_break_config_first_break_eligible_secs','from.name':'from_name','recommended_encoder_settings.streaming_protocol':'recommended_encoder_settings_streaming_protocol','recommended_encoder_settings.video_codec_settings.codec':'recommended_encoder_settings_video_codec_settings_codec','recommended_encoder_settings.video_codec_settings.max_bitrate':'recommended_encoder_settings_video_codec_settings_max_bitrate','recommended_encoder_settings.video_codec_settings.max_width':'recommended_encoder_settings_video_codec_settings_max_width','recommended_encoder_settings.video_codec_settings.max_height':'recommended_encoder_settings_video_codec_settings_max_height','recommended_encoder_settings.video_codec_settings.max_framerate':'recommended_encoder_settings_video_codec_settings_max_framerate','recommended_encoder_settings.video_codec_settings.profile':'recommended_encoder_settings_video_codec_settings_profile','recommended_encoder_settings.video_codec_settings.level':'recommended_encoder_settings_video_codec_settings_level','recommended_encoder_settings.video_codec_settings.scan_mode':'recommended_encoder_settings_video_codec_settings_scan_mode','recommended_encoder_settings.video_codec_settings.rate_control_mode':'recommended_encoder_settings_video_codec_settings_rate_control_mode','recommended_encoder_settings.video_codec_settings.gop_size_in_seconds':'recommended_encoder_settings_video_codec_settings_gop_size_in_seconds','recommended_encoder_settings.video_codec_settings.gop_type':'recommended_encoder_settings_video_codec_settings_gop_type','recommended_encoder_settings.video_codec_settings.gop_closed':'recommended_encoder_settings_video_codec_settings_gop_closed','recommended_encoder_settings.audio_codec_settings.codec':'recommended_encoder_settings_audio_codec_settings_codec','recommended_encoder_settings.audio_codec_settings.bitrate':'recommended_encoder_settings_audio_codec_settings_bitrate','recommended_encoder_settings.audio_codec_settings.channels':'recommended_encoder_settings_audio_codec_settings_channels','recommended_encoder_settings.audio_codec_settings.samplerate':'recommended_encoder_settings_audio_codec_settings_samplerate','comments.data':'comments_data','comments.summary.total_count':'comments_summary_total_count','reactions.data':'reactions_data','reactions.summary.total_count':'reactions_summary_total_count','errors.data':'errors_data','crosspost_shared_pages.data':'crosspost_shared_pages_data','crossposted_broadcasts.data':'crossposted_broadcasts_data'}, inplace = True)
            live_vids_dataframe['id_fb_user'] = id_fb_user
            #live_vids_dataframe['id_media'] = str(id_fb_user) + "_" + str(live_vids_dataframe['id_media'])
            #live_vids_dataframe['id_media'] = live_vids_dataframe['id_media'].astype(str)

            live_vids_dataframe = frame_validate(live_vids_column_order, live_vids_dataframe)
            
            # Process columns to set as integer
            live_vids_columns_int = ['id_fb_user','live_views','ad_break_config_default_ad_break_duration','ad_break_config_failure_reason_polling_interval','ad_break_config_preparing_duration','ad_break_config_viewer_count_threshold','ad_break_config_time_between_ad_breaks_secs','ad_break_config_first_break_eligible_secs','recommended_encoder_settings_video_codec_settings_max_bitrate','recommended_encoder_settings_video_codec_settings_max_width','recommended_encoder_settings_video_codec_settings_max_height','recommended_encoder_settings_video_codec_settings_max_framerate','recommended_encoder_settings_video_codec_settings_gop_size_in_seconds','recommended_encoder_settings_audio_codec_settings_bitrate','recommended_encoder_settings_audio_codec_settings_channels','recommended_encoder_settings_audio_codec_settings_samplerate','comments_summary_total_count','reactions_summary_total_count']

            # Process columns to set as string
            live_vids_columns_str = ['video_live_id','id_media','media_type','ad_break_failure_reason','dash_ingest_url','dash_preview_url','description','embed_html','ingest_streams','overlay_url','secure_stream_url','status','stream_url','title','permalink_url','ad_break_config_guide_url','ad_break_config_onboarding_url','from_name','recommended_encoder_settings_streaming_protocol','recommended_encoder_settings_video_codec_settings_codec','recommended_encoder_settings_video_codec_settings_profile','recommended_encoder_settings_video_codec_settings_scan_mode','recommended_encoder_settings_video_codec_settings_rate_control_mode','recommended_encoder_settings_video_codec_settings_gop_type','recommended_encoder_settings_audio_codec_settings_codec','comments_data','reactions_data','errors_data','crosspost_shared_pages_data','crossposted_broadcasts_data','recommended_encoder_settings_video_codec_settings_level']



            # Transform column types
            live_vids_dataframe[live_vids_columns_str] = live_vids_dataframe[live_vids_columns_str].astype(str)
            live_vids_dataframe[live_vids_columns_int] = live_vids_dataframe[live_vids_columns_int].fillna(0).astype(int)
            live_vids_dataframe['creation_time'] = pd.to_datetime(live_vids_dataframe['creation_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            live_vids_dataframe['broadcast_start_time'] = pd.to_datetime(live_vids_dataframe['broadcast_start_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
            live_vids_dataframe['process_timestamp'] = pd.to_datetime(live_vids_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')
    
            # Upload data to BigQuery
            bigquery_config_live_vids_info(live_vids_dataframe[live_vids_column_order], 'live_videos_info', fb_live_video_info_table)

    elif process_type in ('weekly', 'monthly', 'daily-week'):
            
            """
            Get video general info
            """
            
            # Instantiate an empty list to store video lists info
            video_datainfo = []
            
            # Iterate over each element in the video_list_id_list
            for video_id in vids_id_list:
                
                # Build request URL with proper parameters
                video_url_info = f'https://graph.facebook.com/v15.0/{video_id}'+'?fields=ad_breaks,backdated_time,backdated_time_granularity,content_category,content_tags,created_time,custom_labels,description,embed_html,embeddable,event,format,from,icon,id,is_crosspost_video,is_crossposting_eligible,is_episode,is_instagram_eligible,is_reference_only,length,live_status,music_video_copyright,place,post_views,premiere_living_room_status,privacy,published,scheduled_publish_time,source,status,title,universal_video_id,updated_time,views,captions{create_time,is_auto_generated,is_default,locale,locale_name,uri},comments.limit(0).summary(total_count),likes.limit(0).summary(total_count),permalink_url,picture,poll_settings{id,video_poll_www_placement,was_live_voting_enabled},polls{id,question,show_results,status,poll_options},tags,thumbnails{height,id,is_preferred,name,scale,uri,width}'+f'&access_token={token}'
                
                # Try to send the request to the API and Return JSON object of the response
                video_data = get_info_from_url(video_url_info,'video_info')
                
                # Get the keys from the video_list dictionary
                video_exception_keys = video_data.keys()
                
                #Replace some columns with values
                if 'id' in video_exception_keys:
                    video_data['id'] = str(video_data['id'])
                
                # Clean name
                if 'description' in video_exception_keys:
                    video_data['description'] = video_data['description'].replace('\n', ' ').replace('  ', ' ')
                    
                # Clean timestamp objects from the response
                #if 'created_time' in video_exception_keys:            
                #    video_data['created_time'] = video_data['created_time'][:-5]
                #    video_clean_timestamp = datetime.strptime(video_data['created_time'], "%Y-%m-%dT%H:%M:%S")
                #    video_data['created_time'] = str(datetime.strptime(str(video_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
                
                # Clean updated time field
                #if 'updated_time' in video_exception_keys:
                #    video_data['updated_time'] = video_data['updated_time'][:-5]
                #    video_up_clean_timestamp = datetime.strptime(video_data['updated_time'], "%Y-%m-%dT%H:%M:%S")
                #    video_data['updated_time'] = str(datetime.strptime(str(video_up_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
                    
                # Clean status field
                #if 'status' in video_exception_keys and 'publishing_phase' in video_exception_keys and 'publish_time' in video_exception_keys:
                #    video_data['status']['publishing_phase']['publish_time'] = video_data['status']['publishing_phase']['publish_time'][:-5]
                #    status_video_clean_timestamp = datetime.strptime(video_data['status']['publishing_phase']['publish_time'], "%Y-%m-%dT%H:%M:%S")
                #    video_data['status']['publishing_phase']['publish_time'] = str(datetime.strptime(str(status_video_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
                
                # Clean backdated_time field
                #if 'backdated_time' in video_exception_keys:
                #    video_data['backdated_time'] = video_data['backdated_time'][:-5]
                #    video_back_clean_timestamp = datetime.strptime(video_data['backdated_time'], "%Y-%m-%dT%H:%M:%S")
                #    video_data['backdated_time'] = str(datetime.strptime(str(video_back_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
                    
                # Append the video_data to video_data info list               
                video_datainfo.append(video_data)
            
            # Transform json object to a dataframe
            video_dataframe = pd.json_normalize(video_datainfo)
            
            # If there are elements in the dataframe
            if len(video_dataframe) > 0:

                # Add process timestamp and type to dataframe
                video_dataframe.insert(0,'media_type','video')
                video_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
            
            # Rename columns
            video_dataframe.rename(columns = {'from.id':'id_fb_user','id':'id_media','from.name':'from_name','privacy.allow':'privacy_allow','privacy.deny':'privacy_deny','privacy.description':'privacy_description','privacy.friends':'privacy_friends','privacy.networks':'privacy_networks','privacy.value':'privacy_value','status.video_status':'status_video_status','status.uploading_phase.status':'status_uploading_phase_status','status.processing_phase.status':'status_processing_phase_status','status.publishing_phase.status':'status_publishing_phase_status','status.publishing_phase.publish_status':'status_publishing_phase_publish_status','status.publishing_phase.publish_time':'status_publishing_phase_publish_time','comments.data':'comments_data','comments.summary.total_count':'comments_summary_total_count','likes.data':'likes_data','likes.summary.total_count':'likes_summary_total_count','thumbnails.data':'thumbnails_data','captions.data':'captions_data'}, inplace = True)
            video_dataframe['id_fb_user'] = id_fb_user

            # Column order to match database table
            video_column_order = ['id_fb_user','id_media','process_timestamp','media_type','created_time','ad_breaks','content_category','description','embed_html','embeddable','format','icon','is_crosspost_video','is_crossposting_eligible','is_episode','is_instagram_eligible','is_reference_only','length','live_status','post_views','published','source','title','updated_time','views','permalink_url','picture','from_name','privacy_allow','privacy_deny','privacy_description','privacy_friends','privacy_networks','privacy_value','status_video_status','status_uploading_phase_status','status_processing_phase_status','status_publishing_phase_status','status_publishing_phase_publish_status','status_publishing_phase_publish_time','comments_data','comments_summary_total_count','likes_data','likes_summary_total_count','thumbnails_data','custom_labels','captions_data','universal_video_id','backdated_time','backdated_time_granularity']

        
            if len(video_dataframe) > 0:

                video_dataframe = frame_validate(video_column_order, video_dataframe)

                # Process columns to set as integer
                unique_vids_columns_int = ['post_views','views','comments_summary_total_count','likes_summary_total_count']

                # Process columns to set as string
                unique_vids_columns_str = ['id_media','media_type','ad_breaks','content_category','description','embed_html','format','icon','live_status','source','title','permalink_url','picture','from_name','privacy_allow','privacy_deny','privacy_description','privacy_friends','privacy_networks','privacy_value','status_video_status','status_uploading_phase_status','status_processing_phase_status','status_publishing_phase_status','status_publishing_phase_publish_status','comments_data','likes_data','thumbnails_data','custom_labels','captions_data','universal_video_id']

                # Transform column types
                video_dataframe[unique_vids_columns_str] = video_dataframe[unique_vids_columns_str].astype(str)
                video_dataframe[unique_vids_columns_int] = video_dataframe[unique_vids_columns_int].fillna(0).astype(int)
                video_dataframe['created_time'] = pd.to_datetime(video_dataframe['created_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
                video_dataframe['updated_time'] = pd.to_datetime(video_dataframe['updated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
                video_dataframe['status_publishing_phase_publish_time'] = pd.to_datetime(video_dataframe['status_publishing_phase_publish_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
                video_dataframe['backdated_time'] = pd.to_datetime(video_dataframe['backdated_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
                video_dataframe['process_timestamp'] = pd.to_datetime(video_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')

                # Upload data to BigQuery
                bigquery_config_video_info(video_dataframe[video_column_order],'video_info',fb_video_info_table)


            #live video info
            
            live_vids_datainfo = []
            live_videos_id_list_for_insights = []
            
            for live_vids_id in live_vids_id_list:
                
                # Build request URL with proper parameters
                live_vids_url_info = f'https://graph.facebook.com/v15.0/{live_vids_id}'+'?fields=ad_break_config,ad_break_failure_reason,broadcast_start_time,copyright,creation_time,dash_ingest_url,dash_preview_url,description,embed_html,from,id,ingest_streams,is_reference_only,is_manual_mode,overlay_url,live_views,planned_start_time,seconds_left,recommended_encoder_settings,secure_stream_url,status,stream_url,title,targeting,video,blocked_users.limit(0),comments.limit(0).summary(total_count),reactions.limit(0).summary(total_count),crosspost_shared_pages,crossposted_broadcasts,errors{creation_time,error_code,error_message,error_type},permalink_url,polls'+f'&access_token={token}'
                
                # Try to send the request to the API and Return JSON object of the response
                live_vids_data = get_info_from_url(live_vids_url_info, 'live_vids_info')
                

                live_vids_exception_keys = live_vids_data.keys()
                
                # Replace some columns with values
                if 'id' in live_vids_exception_keys:
                #    print(str(live_vids_data['id']))
                #    print(str(live_vids_data['id']))
                #    print(str(live_vids_id))
                    live_vids_data['id'] = str(live_vids_data['id'])
                 
                # Clean name
                if 'description' in live_vids_exception_keys:
                    live_vids_data['description'] = live_vids_data['description'].replace('\n', ' ').replace('  ', ' ')
                    
                # Clean the timestamp type objects from the response
                #if 'creation_time' in live_vids_exception_keys:            
                #    live_vids_data['creation_time'] = live_vids_data['creation_time'][:-5]
                #    live_vids_clean_timestamp = datetime.strptime(live_vids_data['creation_time'], "%Y-%m-%dT%H:%M:%S")
                #    live_vids_data['creation_time'] = str(datetime.strptime(str(live_vids_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'

                #if 'broadcast_start_time' in live_vids_exception_keys:  
                #    live_vids_data['broadcast_start_time'] = live_vids_data['broadcast_start_time'][:-5]
                #    live_vids_broad_clean_timestamp = datetime.strptime(live_vids_data['broadcast_start_time'], "%Y-%m-%dT%H:%M:%S")
                #    live_vids_data['broadcast_start_time'] = str(datetime.strptime(str(live_vids_broad_clean_timestamp), "%Y-%m-%d %H:%M:%S")) + '.000000'
                
                
                live_videos_id_list_for_insights.append(live_vids_data['video']['id'])      
                live_vids_datainfo.append(live_vids_data)
            
            # Column order to match database table
            live_vids_column_order = ['id_fb_user','id_media','process_timestamp','media_type','creation_time','ad_break_failure_reason',
                                    'broadcast_start_time','dash_ingest_url','dash_preview_url','description','embed_html',
                                    'ingest_streams','is_reference_only','overlay_url','live_views','seconds_left','secure_stream_url',
                                    'status','stream_url','title','permalink_url','ad_break_config_is_eligible_to_onboard',
                                    'ad_break_config_guide_url','ad_break_config_onboarding_url','ad_break_config_is_enabled',
                                    'ad_break_config_default_ad_break_duration','ad_break_config_failure_reason_polling_interval',
                                    'ad_break_config_preparing_duration','ad_break_config_viewer_count_threshold',
                                    'ad_break_config_time_between_ad_breaks_secs','ad_break_config_first_break_eligible_secs',
                                    'from_name','recommended_encoder_settings_streaming_protocol',
                                    'recommended_encoder_settings_video_codec_settings_codec',
                                    'recommended_encoder_settings_video_codec_settings_max_bitrate',
                                    'recommended_encoder_settings_video_codec_settings_max_width',
                                    'recommended_encoder_settings_video_codec_settings_max_height',
                                    'recommended_encoder_settings_video_codec_settings_max_framerate',
                                    'recommended_encoder_settings_video_codec_settings_profile',
                                    'recommended_encoder_settings_video_codec_settings_level',
                                    'recommended_encoder_settings_video_codec_settings_scan_mode',
                                    'recommended_encoder_settings_video_codec_settings_rate_control_mode',
                                    'recommended_encoder_settings_video_codec_settings_gop_size_in_seconds',
                                    'recommended_encoder_settings_video_codec_settings_gop_type',
                                    'recommended_encoder_settings_video_codec_settings_gop_closed',
                                    'recommended_encoder_settings_audio_codec_settings_codec',
                                    'recommended_encoder_settings_audio_codec_settings_bitrate',
                                    'recommended_encoder_settings_audio_codec_settings_channels',
                                    'recommended_encoder_settings_audio_codec_settings_samplerate','video_live_id','comments_data',
                                    'comments_summary_total_count','reactions_data','reactions_summary_total_count','errors_data',
                                    'crosspost_shared_pages_data','crossposted_broadcasts_data']


            # Transform json object to a dataframe
            live_vids_dataframe = pd.json_normalize(live_vids_datainfo)
            
            # Add process timestamp and post_type to dataframe
            if len(live_vids_dataframe) > 0:

                live_vids_dataframe.insert(0, 'media_type', 'live_video')
                live_vids_dataframe.insert(0, 'process_timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
            
                # Rename ig_user_column
                
                live_vids_dataframe.rename(columns = {'id':'video_live_id', 'from.id':'id_fb_user','video.id':'id_media','ad_break_config.is_eligible_to_onboard':'ad_break_config_is_eligible_to_onboard','ad_break_config.guide_url':'ad_break_config_guide_url','ad_break_config.onboarding_url':'ad_break_config_onboarding_url','ad_break_config.is_enabled':'ad_break_config_is_enabled','ad_break_config.default_ad_break_duration':'ad_break_config_default_ad_break_duration','ad_break_config.failure_reason_polling_interval':'ad_break_config_failure_reason_polling_interval','ad_break_config.preparing_duration':'ad_break_config_preparing_duration','ad_break_config.viewer_count_threshold':'ad_break_config_viewer_count_threshold','ad_break_config.time_between_ad_breaks_secs':'ad_break_config_time_between_ad_breaks_secs','ad_break_config.first_break_eligible_secs':'ad_break_config_first_break_eligible_secs','from.name':'from_name','recommended_encoder_settings.streaming_protocol':'recommended_encoder_settings_streaming_protocol','recommended_encoder_settings.video_codec_settings.codec':'recommended_encoder_settings_video_codec_settings_codec','recommended_encoder_settings.video_codec_settings.max_bitrate':'recommended_encoder_settings_video_codec_settings_max_bitrate','recommended_encoder_settings.video_codec_settings.max_width':'recommended_encoder_settings_video_codec_settings_max_width','recommended_encoder_settings.video_codec_settings.max_height':'recommended_encoder_settings_video_codec_settings_max_height','recommended_encoder_settings.video_codec_settings.max_framerate':'recommended_encoder_settings_video_codec_settings_max_framerate','recommended_encoder_settings.video_codec_settings.profile':'recommended_encoder_settings_video_codec_settings_profile','recommended_encoder_settings.video_codec_settings.level':'recommended_encoder_settings_video_codec_settings_level','recommended_encoder_settings.video_codec_settings.scan_mode':'recommended_encoder_settings_video_codec_settings_scan_mode','recommended_encoder_settings.video_codec_settings.rate_control_mode':'recommended_encoder_settings_video_codec_settings_rate_control_mode','recommended_encoder_settings.video_codec_settings.gop_size_in_seconds':'recommended_encoder_settings_video_codec_settings_gop_size_in_seconds','recommended_encoder_settings.video_codec_settings.gop_type':'recommended_encoder_settings_video_codec_settings_gop_type','recommended_encoder_settings.video_codec_settings.gop_closed':'recommended_encoder_settings_video_codec_settings_gop_closed','recommended_encoder_settings.audio_codec_settings.codec':'recommended_encoder_settings_audio_codec_settings_codec','recommended_encoder_settings.audio_codec_settings.bitrate':'recommended_encoder_settings_audio_codec_settings_bitrate','recommended_encoder_settings.audio_codec_settings.channels':'recommended_encoder_settings_audio_codec_settings_channels','recommended_encoder_settings.audio_codec_settings.samplerate':'recommended_encoder_settings_audio_codec_settings_samplerate','comments.data':'comments_data','comments.summary.total_count':'comments_summary_total_count','reactions.data':'reactions_data','reactions.summary.total_count':'reactions_summary_total_count','errors.data':'errors_data','crosspost_shared_pages.data':'crosspost_shared_pages_data','crossposted_broadcasts.data':'crossposted_broadcasts_data'}, inplace = True)
                live_vids_dataframe['id_fb_user'] = id_fb_user
                #live_vids_dataframe['id_media'] = str(live_vids_dataframe['id_media'])
                #live_vids_dataframe['id_media'] = live_vids_dataframe['id_media'].astype(str)

                live_vids_dataframe = frame_validate(live_vids_column_order, live_vids_dataframe)
                
                # Process columns to set as integer
                live_vids_columns_int = ['id_fb_user','live_views','ad_break_config_default_ad_break_duration','ad_break_config_failure_reason_polling_interval','ad_break_config_preparing_duration','ad_break_config_viewer_count_threshold','ad_break_config_time_between_ad_breaks_secs','ad_break_config_first_break_eligible_secs','recommended_encoder_settings_video_codec_settings_max_bitrate','recommended_encoder_settings_video_codec_settings_max_width','recommended_encoder_settings_video_codec_settings_max_height','recommended_encoder_settings_video_codec_settings_max_framerate','recommended_encoder_settings_video_codec_settings_gop_size_in_seconds','recommended_encoder_settings_audio_codec_settings_bitrate','recommended_encoder_settings_audio_codec_settings_channels','recommended_encoder_settings_audio_codec_settings_samplerate','comments_summary_total_count','reactions_summary_total_count']

                # Process columns to set as string
                live_vids_columns_str = ['video_live_id','id_media','media_type','ad_break_failure_reason','dash_ingest_url','dash_preview_url','description','embed_html','ingest_streams','overlay_url','secure_stream_url','status','stream_url','title','permalink_url','ad_break_config_guide_url','ad_break_config_onboarding_url','from_name','recommended_encoder_settings_streaming_protocol','recommended_encoder_settings_video_codec_settings_codec','recommended_encoder_settings_video_codec_settings_profile','recommended_encoder_settings_video_codec_settings_scan_mode','recommended_encoder_settings_video_codec_settings_rate_control_mode','recommended_encoder_settings_video_codec_settings_gop_type','recommended_encoder_settings_audio_codec_settings_codec','comments_data','reactions_data','errors_data','crosspost_shared_pages_data','crossposted_broadcasts_data','recommended_encoder_settings_video_codec_settings_level']

                # Transform column types
                live_vids_dataframe[live_vids_columns_str] = live_vids_dataframe[live_vids_columns_str].astype(str)
                live_vids_dataframe[live_vids_columns_int] = live_vids_dataframe[live_vids_columns_int].fillna(0).astype(int)
                live_vids_dataframe['creation_time'] = pd.to_datetime(live_vids_dataframe['creation_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
                live_vids_dataframe['broadcast_start_time'] = pd.to_datetime(live_vids_dataframe['broadcast_start_time'], format = '%Y-%m-%dT%H:%M:%S+0000')
                live_vids_dataframe['process_timestamp'] = pd.to_datetime(live_vids_dataframe['process_timestamp'], format = '%Y-%m-%d %H:%M:%S.%f')
                
                # Upload data to BigQuery
                bigquery_config_live_vids_info(live_vids_dataframe[live_vids_column_order], 'live_videos_info', fb_live_video_info_table)

    return live_videos_id_list_for_insights

def run_job():
    
    st = time.time()

    global token, id_fb_user,fb_page_insights_table,fb_post_insights_table,fb_page_info_table,fb_post_info_table,fb_albums_info_table,fb_photo_albums_info_table,fb_video_lists_info_table,fb_video_info_table,fb_live_video_info_table,slack_webhook
    token, id_fb_user,fb_page_insights_table,fb_post_insights_table,fb_page_info_table,fb_post_info_table,fb_albums_info_table,fb_photo_albums_info_table,fb_video_lists_info_table,fb_video_info_table,fb_live_video_info_table,slack_webhook = read_config_files('nmas-prod-conf')
    
    process_type = 'daily'

    since = datetime.today() - timedelta(days = 1)
    since = str(since.date())
    until = datetime.today() - timedelta(days = 0)
    until = str(until.date())

    #since = '2023-07-08'
    #until = '2023-07-09'
       
    #delete_duplicate_data_bigquery_tables(since, fb_page_info_table, fb_page_insights_table, fb_albums_info_table, fb_photo_albums_info_table, fb_video_lists_info_table, fb_video_info_table, fb_live_video_info_table, fb_post_info_table, fb_post_insights_table)
    #print('Deleted data from current date successfully.')

    # Get all objects lists other than page
    posts_id_list,ads_post_id_list,albums_id_list,albums_photo_id_list,video_lists_id_list,vids_id_list,live_vids_id_list,reels_vids_id_list,all_id_list = get_media_id_list(process_type, id_fb_user, since, until, token)
    
    # Update user_info data
    get_user_info(id_fb_user, since, 'user_info', fb_page_info_table)
    get_page_insights(since, until, token, id_fb_user, fb_page_insights_table)
    get_post_info(id_fb_user, since, until, token, posts_id_list, ads_post_id_list, fb_post_info_table)
    photo_info(process_type, id_fb_user, since, until, token, albums_id_list, albums_photo_id_list, fb_albums_info_table, fb_photo_albums_info_table)
    live_videos_id_list_for_insights = video_info(process_type,id_fb_user,since,until,token,video_lists_id_list,vids_id_list,live_vids_id_list,reels_vids_id_list,fb_video_lists_info_table,fb_video_info_table,fb_live_video_info_table)
    
    all_id_list_for_insights = live_videos_id_list_for_insights+all_id_list
    all_post_list = unique_list(id_fb_user, all_id_list_for_insights)
    #all_post_list = posts_id_list + ads_post_id_list
    #list=[]

    print('Getting insights for unique_list with {} elements'.format(len(all_post_list)))

    # Get all post insights
    #get_post_insights(token, since, sample(all_post_list, 10), id_fb_user, fb_post_insights_table)
    get_post_insights(token, since, all_post_list, id_fb_user, fb_post_insights_table)

    #run_insights(list, token, id_fb_user, fb_page_insights_table,fb_post_impressions_table,fb_post_reactions_table,fb_post_video_table,fb_post_activity_table,fb_page_info_table,fb_post_info_table,fb_albums_info_table,fb_photo_albums_info_table,fb_video_lists_info_table,fb_video_info_table,fb_live_video_info_table,slack_webhook)
    #print(len(all_post_list))
    
    et = time.time()
    #print(f"Finish in: {round(et - st, 1)} seconds. Total elements: {len(all_post_list)}")
    slack_message = f'SUCCESS: fb-data-update {process_type} job ran successfully in {round(et - st, 1)} seconds. Total elements processed: {len(all_post_list)}, between {since} and {until}.'
    post_slack_updates(slack_message)
    print(slack_message)

run_job()