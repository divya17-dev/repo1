# import the library
import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv
import datetime
from bs4 import BeautifulSoup
import threading
import time
import schedule
import base64
import hashlib
import pymongo
from pathlib import Path
from openai import AzureOpenAI
import queue
import shutil
import glob
import re
import docx
from collections import deque
from unstructured.partition.xlsx import partition_xlsx
from unstructured.partition.text import partition_text
from unstructured.partition.pdf import partition_pdf
from unstructured.partition.image import partition_image
from unstructured.partition.html import partition_html
from unstructured.partition.docx import partition_docx
from unstructured.partition.doc import partition_doc
from unstructured.partition.csv import partition_csv
from unstructured.documents.elements import NarrativeText
from concurrent.futures import ThreadPoolExecutor
from requests.auth import HTTPBasicAuth
from PIL import Image
import io
import sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from sentence_transformers import SentenceTransformer
import traceback
import urllib3
import xml


try:
#Load the environment variable
    
        # load_dotenv()

        # # Client credentials
        # CLIENT_ID = os.getenv('CLIENT_ID')
        # CLIENT_SECRET=os.getenv('CLIENT_SECRET')
        # TENANT_ID=os.getenv('TENANT_ID')
        # EMAIL_ID=os.getenv('email_id')
        # SCOPE = os.getenv('SCOPE')
        # WEBHOOK=os.getenv('WEBHOOK_URL')
        monitoring_started = False
        # Client credentials
        CLIENT_ID = os.environ['CLIENT_ID']
        CLIENT_SECRET=os.environ['CLIENT_SECRET']
        TENANT_ID=os.environ['TENANT_ID']
        EMAIL_ID=os.environ['email_id']
        SCOPE = os.environ['SCOPE']
        WEBHOOK=os.environ['WEBHOOK_URL']

        # Hold a email
        q = queue.Queue()
        processing_queue =deque()
        processed_files = set()
        
        
       
        
         
        # # # Database
        # db_username=os.environ.get("db_username",db_username1)
        # db_password=os.environ.get("db_password",db_password1)
        # db_host=os.environ.get("db_host",db_host1)
        # db_port = int(os.environ.get("db_port",db_port1))
        # # Database
        db_username=os.environ.get("db_username")
        db_password=os.environ.get("db_password")
        db_host=os.environ.get("db_host")
        db_port = int(os.environ.get("db_port",10255))

        # Construct MongoDB connection string
        connection_string = f"mongodb://{db_username}:{db_password}@{db_host}:{db_port}/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@podatabase@"
        print("Using connection string:", connection_string)  # it's working
        # Initialize MongoDB client
        client = pymongo.MongoClient(connection_string,maxPoolSize=10)
        print("client")
        server_info = client.server_info()
        print("server_info")
        client.admin.command("ping")  # Forces MongoDB handshake
        print("Connected successfully")
        print("Connected to MongoDB Version:", server_info.get("version", "Unknown"))

        #create a file
        SUBSCRIPTION_FILE = "subscription.json"


        # Print full server info to see available fields
        print(server_info)
        # # Database for Confirmation
        db = client["Confirmation"]
        collection = db["Details"]
        proof_collection = db["Proof"]
        missing_collection=db["Missing"]

        # Testing database
        # db = client["Testing_Confirmation"]
        # collection = db["Testing_Details"]
        # proof_collection = db["Testing_Proof"]
        # missing_collection=db["TestingMissing"]

        ATTACHMENT_DIR = "attachments"

        # # Unique variable
        seen_emails = set()
 
     
        def configure_openai1():
            try:
                client = AzureOpenAI(
                    api_key=os.environ["subscription_key"],
                    azure_endpoint=os.environ["endpoint"],
                    api_version=os.environ["api_version"]
                )
                return client
            except Exception as e:
                print(f"Azure OpenAI configuration error 1: {e}")
                return None

        def configure_openai2():
            try:
                client = AzureOpenAI(
                    api_key=os.environ["subscription_key"],
                    azure_endpoint=os.environ["endpoint"],
                    api_version=os.environ["api_version"]
                )
                return client
            except Exception as e:
                print(f"Azure OpenAI configuration error 2: {e}")
                return None

        # 60% OpenAI1, 40% OpenAI2 with fallback logic
        def generate_response_balanced(prompt):
            use_first = random.choices([True, False], weights=[0.6, 0.4])[0]
            model_name = os.environ["model_name"]   # or gpt-4-turbo if you want
            try:
                if use_first:
                    print("Using Azure OpenAI 1")
                    client = configure_openai1()
                    response = client.chat.completions.create(
                        model=os.environ["model_name"],
                        temperature=0.0,
                        top_p=0.0,
                        messages=[
                            {"role": "system", "content": "You are a helpful assistant."},
                            {"role": "user", "content": prompt}
                        ],
                        max_tokens=4096
                    )
                    return response.choices[0].message.content.strip()
                else:
                    print("Using Azure OpenAI 2")
                    client = configure_openai2()
                    response = client.chat.completions.create(
                        model=os.environ["model_name"],
                        temperature=0.0,
                        top_p=0.0,
                        messages=[
                            {"role": "system", "content": "You are a helpful assistant."},
                            {"role": "user", "content": prompt}
                        ],
                        max_tokens=4096
                    )
                    return response.choices[0].message.content.strip()
            except Exception as e:
                print(f"Error with {'Azure OpenAI 1' if use_first else 'Azure OpenAI 2'}: {e}")
                try:
                    if use_first:
                        print("Fallback to Azure OpenAI 2")
                        client = configure_openai2()
                    else:
                        print("Fallback to Azure OpenAI 1")
                        client = configure_openai1()

                    response = client.chat.completions.create(
                        model=model_name,
                        temperature=0.0,
                        messages=[
                            {"role": "system", "content": "You are a helpful assistant."},
                            {"role": "user", "content": prompt}
                        ],
                        max_tokens=4096
                    )
                    return response.choices[0].message.content.strip()
                except Exception as fallback_error:
                    return f"Fallback also failed: {fallback_error}"         
        
    
    
    
    
        def remove_signature_using_gemini(email_body):
            try:
            

                prompt = f"""
                You are an AI designed to extract the main content from an email while removing unnecessary text like email signatures, disclaimers, and quoted messages from replies or forwards.

                **Instructions:**
                - Keep only the latest meaningful content.
                - Remove email signatures, legal disclaimers, and footers.
                - Remove quoted replies (e.g., 'On Jan 1, 2024, John Doe wrote:')
                - Ensure that only the latest message remains.

                **Email Content:**
                {email_body}

                **Cleaned Email (without signature, disclaimers, and old replies/forwards):**
                """

                try:
                    response = generate_response_balanced(prompt)
                    if hasattr(response, "text"):
                        clean_body = response.text.strip()
                    elif isinstance(response, str):
                        clean_body = response.strip()
                    else:
                        print("Unexpected response format:", response)
                        clean_body = "{}"
                    time.sleep(1)  # Prevent hitting API rate limits
                    return clean_body
                except Exception as e:
                    print(f"Error cleaning email body with Azure OpenAI: {e}")
                    return email_body  # Fallback to original if Gemini fails
            except Exception as e:
                print(f"remove_signature_using Azure OpenAI error: {e}")
                label = "Other"
        stop_event = threading.Event()
    
        
       
        def worker():
            """Worker to process emails one-by-one."""
            while not stop_event.is_set():
                try:
                    email = q.get(timeout=5)

                    email_id = email["id"]
                    
                    if email["id"] in seen_emails:
                        print(f"Already processed: {email['id']}")
                        q.task_done()
                        continue

                    # if email["id"] in seen_emails:
                    #     print(f"Already processed in memory, skipping: {email.get('subject', 'No Subject')}")
                    #     q.task_done()
                    #     continue 
                    
                    seen_emails.add(email["id"])
                    subject = email.get("subject", "No Subject")
                    from_email = email.get("from", {}).get("emailAddress", {}).get("address", "N/A")
                    to_recipients = [r["emailAddress"]["address"] for r in email.get("toRecipients", [])]
                    cc_recipients = [r["emailAddress"]["address"] for r in email.get("ccRecipients", [])]
                    bcc_recipients = [r["emailAddress"]["address"] for r in email.get("bccRecipients", [])]
                    received_time = email.get("receivedDateTime", "")

                    received_utc = datetime.datetime.strptime(received_time, "%Y-%m-%dT%H:%M:%SZ")
                    received_ist = received_utc + datetime.timedelta(hours=5, minutes=30)
                    received_ist_str = received_ist.strftime("%Y-%m-%d %H:%M:%S")
                    has_attachments = email.get("hasAttachments", True)

                    print(f"\nProcessing Email")
                    print(f"From: {from_email}")
                    print(f"To: {', '.join(to_recipients)}")
                    print(f"CC: {', '.join(cc_recipients)}")
                    print(f"BCC: {', '.join(bcc_recipients)}")
                    print(f"Received At: {received_ist_str}")
                    print(f"Subject: {subject}")

                    clean_body=None
                    if not email.get("hasAttachments"):
                        raw_body = email.get("body", {}).get("content", "")
                        plain_body = BeautifulSoup(raw_body, "html.parser").get_text(separator="\n").strip()
                        clean_body = remove_signature_using_gemini(plain_body)
                        print(f"Body:\n{clean_body}")
                    else:
                        print("Email has attachments. Skipping body extraction.")

                    eml_path, short_id = save_email_as_eml(access_token, email_id,subject, EMAIL_ID)
                    print(f"Saved EML at: {eml_path} (ID: {short_id})")
                    base64_eml = convert_file_to_base64(eml_path)
                    classify_email(subject=subject, body=clean_body , has_attachments=has_attachments, email_id=email_id, access_token=access_token, received_datetime=received_ist_str,sender_email=from_email,base64_eml=base64_eml)
                    seen_emails.add(email_id)
                    q.task_done()

                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"Error in worker: {e}")
                    q.task_done()

    
        
        def fetch_with_retry(url, headers, retries=3, backoff_factor=2):
            try:
                for attempt in range(retries):
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        return response
                    elif response.status_code in [429, 500, 502, 503, 504]:  # Retry on these errors
                        time.sleep(backoff_factor ** attempt)  # Exponential backoff
                    else:
                        break  # Stop retrying on other errors
                return None  # Return None if all retries fail
            except Exception as e:
                print(f"fetch_with_retry error: {e}")
    
    # Access the Token
    
        def get_access_token():
            try:
        # Azure AD OAuth token URL
                TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
                # Request payload
                payload = {
                    'grant_type': 'client_credentials',
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET,
                    'scope': SCOPE
                }

                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }

                # Get access token
                response = requests.post(TOKEN_URL, data=payload, headers=headers)
                response_dict = response.json()
                access_token = response_dict.get("access_token")

                if not access_token:
                    print("Failed to retrieve access token:", response_dict)
                    raise Exception("Access token retrieval failed")

                print("Access token retrieved successfully.")

                return access_token
            except Exception as e:
                print(f"get_access_token error: {e}")
        access_token=get_access_token()
    
     
    #Email
        def get_recent_emails(access_token):
            global seen_emails
            seen_emails = set()
            clean_body= None
            try:
                # Microsoft Graph API URL to fetch emails
                filter_time = (datetime.datetime.utcnow() - datetime.timedelta(seconds=30)).isoformat() + "Z"

                EMAILS_URL = f"https://graph.microsoft.com/v1.0/users/{EMAIL_ID}/mailFolders/inbox/messages?$filter=receivedDateTime ge {filter_time}&$orderby=receivedDateTime DESC&$select=subject,body,from,toRecipients,ccRecipients,bccRecipients,receivedDateTime,conversationId,hasAttachments"

                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Accept': 'application/json'
                }


                # Fetch emails
                email_response = requests.get(EMAILS_URL, headers=headers)

                if email_response.status_code == 200:
                    emails = email_response.json()
                    # new_emails = [email for email in emails.get("value", []) if email["id"] not in seen_emails]
                    # new_emails = emails.get("value", [])
                    new_emails = sorted(emails.get("value", []), key=lambda e: e["receivedDateTime"])
                    if new_emails:
                        for email in new_emails:
                            # q.put(email)

                            email_id = email["id"]
                            conversation_id = email["conversationId"]

                            subject = email.get("subject")
                            if subject and subject.lower().startswith(("read:", "accepted:", "declined:", "canceled:")):
                                print(f"Skipping auto-response or read receipt: {subject}")
                                continue
                            elif not subject:
                                print("Email has no subject, skipping.")
                                continue


                            q.put(email)
                            reply_email = get_replies(access_token, conversation_id,EMAIL_ID)
                            print(reply_email)


                            fetch_with_retry(EMAILS_URL, headers, retries=3, backoff_factor=2)

                    else:
                        print("No new emails received in the last 30 seconds.")

                else:
                    print("Error fetching emails:", email_response.json())
            except Exception as e:
                print(f"get_recent_emails error: {e}")
          
    
    
     
        def get_replies(access_token, conversation_id,EMAIL_ID):
            """Fetch replies to a specific conversation thread."""
            try:
                url = f"https://graph.microsoft.com/v1.0/users/{EMAIL_ID}/mailFolders/inbox/messages?$filter=conversationId eq '{conversation_id}'&$orderby=receivedDateTime DESC&$select=id,subject,body,from,receivedDateTime,conversationId,hasAttachments"

                headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
               
                reply_response = requests.get(url, headers=headers)

                if reply_response.status_code == 200:
                    replies = reply_response.json().get("value", [])
                    # new_replies = [reply for reply in replies if reply["id"] not in seen_emails]
                    # new_replies=replies.get("value", [])
                    new_replies = sorted(replies.get("value", []), key=lambda e: e["receivedDateTime"])
                    if new_replies:
                        for reply in new_replies:
                            # q.put(reply)



                            reply_id = reply["id"]

                            re_subject = reply.get("subject", "No Subject")

                            re_subject = reply.get("subject")
                            if re_subject and re_subject.lower().startswith(("read:", "accepted:", "declined:", "canceled:")):
                                print(f"Skipping auto-response or read receipt: {re_subject}")
                                continue
                            elif not re_subject:
                                print("Reply has no subject, skipping.")
                                continue
                            
                            q.put(reply)


                            fetch_with_retry(url, headers, retries=3, backoff_factor=2)


                    else:
                        print(f"No new replies found for conversation ID {conversation_id}.")
                else:
                    print("Failed to fetch replies:", reply_response.json())
            except Exception as e:
                print(f"get_replies error: {e}")

    
    
        def download_attachments(access_token, message_id, save_dir="attachments"):
            from uuid import uuid4
            try:
                PDF_DIR = os.path.join(save_dir, "pdf")
                EXCEL_DIR = os.path.join(save_dir, "excel")
                DOCUMENT_DIR = os.path.join(save_dir, "documents")
                IMAGE_DIR = os.path.join(save_dir, "images")
                CSV_DIR = os.path.join(save_dir, "csv")
                TXT_DIR = os.path.join(save_dir, "txt")
                HTML_DIR = os.path.join(save_dir, "html")

                for folder in [PDF_DIR, EXCEL_DIR, DOCUMENT_DIR, IMAGE_DIR, CSV_DIR, TXT_DIR, HTML_DIR, save_dir]:
                    os.makedirs(folder, exist_ok=True)

                base_url = f"https://graph.microsoft.com/v1.0/users/{EMAIL_ID}/messages/{message_id}/attachments"
                headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

                downloaded_files = []
                url = base_url  # start with base URL

                while url:
                    response = requests.get(url, headers=headers)
                    if response.status_code != 200:
                        print("Failed to fetch attachments:", response.json())
                        break

                    data = response.json()
                    attachments = data.get("value", [])
                    print(f"Found {len(attachments)} attachments")  # if vendor sending 2 attachment  it will show the 2 attachment rather than 6 attachment

                    for attachment in attachments:
                        if attachment.get("@odata.type") == "#microsoft.graph.fileAttachment" and not attachment.get("isInline", True):
                            file_name = attachment["name"].replace("&", "_").replace(" ", "_")
                            file_ext = os.path.splitext(file_name)[1].lower()
                            file_content = base64.b64decode(attachment["contentBytes"])

                            if file_ext in [".pdf"]:
                                dest_folder = PDF_DIR
                            elif file_ext in [".xls", ".xlsx"]:
                                dest_folder = EXCEL_DIR
                            elif file_ext in [".doc", ".docx"]:
                                dest_folder = DOCUMENT_DIR
                            elif file_ext in [".png", ".jpg", ".jpeg"]:
                                dest_folder = IMAGE_DIR
                            elif file_ext in [".csv"]:
                                dest_folder = CSV_DIR
                            elif file_ext in [".txt"]:
                                dest_folder = TXT_DIR
                            elif file_ext in [".html"]:
                                dest_folder = HTML_DIR
                            else:
                                dest_folder = save_dir

                            base_name, ext = os.path.splitext(file_name)
                            counter = 1
                            file_path = os.path.join(dest_folder, file_name)
                            while os.path.exists(file_path):
                                file_path = os.path.join(dest_folder, f"{base_name}_{counter}{ext}")
                                counter += 1

                            with open(file_path, "wb") as f:
                                f.write(file_content)

                            downloaded_files.append(file_path)
                            print(f"Attachment saved: {file_path}")

                            # Enqueue for processing
                            if file_path not in processing_queue and file_path not in processed_files:
                                processing_queue.append(file_path)
                                print(f"Added to processing queue: {file_path}")

                    # Check for pagination
                    url = data.get("@odata.nextLink")
                return downloaded_files
            except Exception as e:
                print(f"download_attachments error: {e}")
            
            
    
    

        def save_email_as_eml(access_token, email_id, subject,EMAIL_ID, ATTACHMENT_DIR="attachments"):
            try:
                EML_DIR = os.path.join(ATTACHMENT_DIR, "eml")

                print(f"Fetching email {email_id} as .eml...")

                # Ensure EMAIL_ID is provided
                if not EMAIL_ID:
                    print("ERROR: EMAIL_ID is missing!")
                    return None, None

                url = f"https://graph.microsoft.com/v1.0/users/{EMAIL_ID}/messages/{email_id}/$value"
                headers = {"Authorization": f"Bearer {access_token}"}

                response = requests.get(url, headers=headers)
                print(f"API Response Code: {response.status_code}")

                if response.status_code == 200:
                    eml_content = response.content
                    if not eml_content:
                        print("No email content received!")
                        return None, None

                    safe_subject = re.sub(r'[^\w\-]', '_', subject)[:100]  # Simple hash alternative
                    filename1 = f"{safe_subject}.eml"
                    print(f"Filename: {filename1}")

                    # Ensure directories exist
                    try:
                        os.makedirs(ATTACHMENT_DIR, exist_ok=True)
                        os.makedirs(EML_DIR, exist_ok=True)
                        print(f"Directories created: {ATTACHMENT_DIR}, {EML_DIR}")
                    except Exception as e:
                        print(f"Error creating directories: {e}")
                        return None, None

                    file_name = filename1.replace("&", "_").replace(" ", "_")
                    file_path = os.path.join(EML_DIR, file_name)
                    print(f"Saving .eml to: {file_path}")

                    # Save the .eml file
                    with open(file_path, "wb",) as f:
                        f.write(eml_content)

                    print(f"Email saved as .eml: {file_path}")
                    return file_path, safe_subject
                else:
                    print(f"Failed to fetch email {email_id}: {response.status_code}")
                    print(f"Response Text: {response.text}")  # Show API error
                    return None, None
            except Exception as e:
                print(f"save_email_as_eml error: {e}")    
    


   

        def process_single_file(file_path):
            try:
                print(f"Processing file: {file_path}")
                file_ext = os.path.splitext(file_path or "")[1].lower()
                extracted_data = None

                if file_ext == ".pdf":
                    extracted_data = process_pdf(file_path)
                    print("Process_file", extracted_data)
                elif file_ext in [".xls", ".xlsx", ".csv"]:
                    extracted_data = process_excel(file_path)
                elif file_ext in [".doc", ".docx"]:
                    extracted_data = process_document(file_path)
                elif file_ext in [".png", ".jpg", ".jpeg"]:
                    extracted_data = process_image(file_path)
                elif file_ext in [".html"]:
                    extracted_data = process_html(file_path)
                elif file_ext in [".txt"]:
                    extracted_data = process_txt(file_path)
                else:
                    extracted_data = {"error": f"Unsupported file type: {file_ext}"}

                processed_files.add(file_path)
                return file_path, extracted_data
            except Exception as e:
                print(f"Error processing file: {e}")
                return None, None
    
    
      
        def process_attachments():
            try:
                if not processing_queue:
                    print("Processing queue is empty. No files to process.")
                    return {}

                print(f"Starting parallel processing of queue: {list(processing_queue)}")
                files_to_process = list(processing_queue)
                processing_queue.clear()

                grouped_results = {}

                with ThreadPoolExecutor(max_workers=6) as executor:
                    futures = [executor.submit(process_single_file, file) for file in files_to_process]
                    for future in as_completed(futures):
                        try:
                            file_path, extracted_data = future.result()
                            # print("extracted_data",extracted_data)
                            parsed = None

                            if isinstance(extracted_data, str):
                                extracted_data = extracted_data.strip()
                                # print("extracted_data1",extracted_data)
                                if extracted_data.startswith("```json"):
                                    extracted_data = re.sub(r"^```(?:json)?\s*|\s*```$", "", extracted_data.strip(), flags=re.MULTILINE)
                                    # print("extracted_data2",extracted_data)
                                    extracted_data=flatten_po_structure(extracted_data)
                                    print("extracted_data2",extracted_data)
                                if extracted_data:
                                    print("extracted_data3",extracted_data)
                                    try:
                                        parsed = clean_and_parse_json(extracted_data)
                                        # print("parsed",parsed)
                                    except json.JSONDecodeError as e:
                                        print(f"Failed to parse JSON from {file_path}: {e}")
                                        continue  # Skip this file but continue processing others
                                else:
                                    print(f"Extracted data is empty from {file_path}")
                                    continue  # Skip this file but continue processing others

                            elif isinstance(extracted_data, (dict, list)):
                                parsed = extracted_data
                                # print("parsed1",parsed)
                            else:
                                print(f"Unrecognized format for extracted data from {file_path}")
                                continue

                            if isinstance(parsed, dict):
                                if all(isinstance(v, dict) and "po_number" in v for v in parsed.values()):
                                    # Order Confirmation format
                                    for po_num, po_data in parsed.items():
                                        normalize_line_items(po_data)
                                        grouped_results[po_num] = po_data

                                elif all(isinstance(v, list) and all(isinstance(i, dict) and "material_code" in i for i in v) for v in parsed.values()):
                                    # BOL format: { "4500001716": [{"material_code": "0016"}] }
                                    for po_num, materials in parsed.items():
                                        bol_data = {
                                            "po_number": po_num,
                                            "label": "Bill of Lading",
                                            "received_datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            "bol_status": False,
                                            "line_items": materials
                                        }
                                        grouped_results[po_num] = bol_data

                                else:
                                    # Fallback: single PO as flat dict
                                    po = parsed.get("po_number")
                                    if po:
                                        grouped_results[po] = parsed

                        except Exception as e:
                            print(f"Error processing future: {e}")

                print("All files processed.",grouped_results)
                return grouped_results
            except Exception as e:
                print(f"Error processing attachments: {e}")
   

        base_folder = "order_confirmation"
        file_name = "CHG_PRD_Material_List.XLSX"
        fallback_excel_path = os.path.join(base_folder, file_name)
    
        def flatten_po_structure(parsed):
            try:
                if (
                    isinstance(parsed, dict)
                    and len(parsed) == 1
                    and isinstance(list(parsed.values())[0], dict)
                    and "header_details" in list(parsed.values())[0]
                ):
                    print("Detected PO wrapper with 'header_details', flattening structure.")
                    inner_data = list(parsed.values())[0]
                    header_details = inner_data.get("header_details", {})
                    # Merge header_details with the rest
                    flattened_data = {**header_details}
                    for key, value in inner_data.items():
                        if key != "header_details":
                            flattened_data[key] = value
        
                    return flattened_data
                return parsed
            except Exception as e:
                print(f"Error in flatten_po_structure: {e}")
                return parsed
    
        def flatten_nested_po(parsed_data):
                """
                Flattens Gemini output like:
                {
                    "4500001696": {
                        "header_details": {...},
                        "line_items": [...]
                    }
                }
                into:
                {
                    "po_number": "4500001696",
                    ...
                    "line_items": [...]
                }
                """
                try:
                    if (
                        isinstance(parsed_data, dict)
                        and len(parsed_data) == 1
                        and isinstance(list(parsed_data.values())[0], dict)
                    ):
                        po_data = list(parsed_data.values())[0]
                        po_number = list(parsed_data.keys())[0]

                        # Flatten header_details if present
                        if "header_details" in po_data:
                            flat = {**po_data["header_details"]}
                            if "line_items" in po_data:
                                flat["line_items"] = po_data["line_items"]
                            return flat

                        # Else assume already flat structure
                        return po_data
                    return parsed_data
                except Exception as e:
                    print(f"Error flattening nested PO: {e}")
                    return parsed_data

        
        
    


        def fetch_sap_master_data(fallback_excel_path=fallback_excel_path):
            """
            Fetch material master data from SAP API.
            If API fails, fallback to reading from Excel file.
            Returns: List of dicts with Material and Description.
            """
            try:
                api_data = []
                api_success = False

                # 1. Try fetching from SAP API
                url = os.environ["master_url"]
                username = os.environ["api_username"]
                password = os.environ["api_password"]
                headers = {
                    'x-csrf-token': 'fetch'
                }

                print("Trying SAP API first...")
                try:
                    response = requests.get(url, headers=headers, auth=HTTPBasicAuth(username, password))
                    response.raise_for_status()

                    data = response.json()
                    results = data.get("d", {}).get("results", [])
                    api_data = [
                        {"Material": item.get("Material", ""), "Description": item.get("Description", "")}
                        for item in results
                    ]

                    if api_data:
                        print(f"Fetched {len(api_data)} records from SAP API.")
                        api_success = True

                except Exception as e:
                    print(f"API fetch failed: {e}")

                # 2. Fallback to Excel if API fails
                if not api_success:
                    print("Falling back to Excel...")
                    try:
                        excel_path = os.path.abspath(fallback_excel_path)
                        df = pd.read_excel(excel_path)
                        excel_data = df[["Material", "Description"]].dropna().to_dict(orient="records")
                        print(f"Loaded {len(excel_data)} records from Excel: {excel_path}")
                        return excel_data
                    except Exception as ex:
                        print(f"Excel fallback failed: {ex}")
                        return []

                else:
                    return api_data
            except Exception as e:
                print(f"Error fetching SAP master data: {e}")
    
    
        def clean_and_parse_json(raw_text):
            try:
                # Step 1: Remove ```json ... ``` if present
                raw_text = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_text.strip(), flags=re.MULTILINE)

                # Step 2: Truncate to last valid }
                last_brace_index = raw_text.rfind('}')
                if last_brace_index != -1:
                    raw_text = raw_text[:last_brace_index + 1]

                # Step 3: Clean bad # symbols inside quoted strings
                def fix_hash_inside_string(match):
                    content = match.group(0)
                    return content.replace("#s", "'s").replace("#", "-")
                
                raw_text = re.sub(r'\".*?\"', fix_hash_inside_string, raw_text)

                # Step 4: Fix bad premature "}," endings inside string fields
                # Look for pattern where text ends with } followed by comma and another quoted key
                raw_text = re.sub(r'\"\},\s*\"', '", "', raw_text)

                # Step 5: Fix multiple JSONs joined together
                if '}{' in raw_text:
                    raw_text = raw_text.replace('}{', '},{')
                    raw_text = f"[{raw_text}]"

                # Step 6: Final safe parse
                return json.loads(raw_text)

            except json.JSONDecodeError as e:
                print(f"JSON parsing failed: {e}")
                print("Problematic content preview:\n", raw_text[:800])
                return {}
    
   
        
        def fetch_sap_data():
                url = os.environ["fetch_url"]
                
                username = os.environ["api_username"]
                password = os.environ["api_password"]
                headers = {
                    'x-csrf-token': 'fetch'
                }
                
                response = requests.get(url, headers=headers, auth=HTTPBasicAuth(username, password))
                

                # Parse the JSON response
                data = json.loads(response.text)
                
                # Extract relevant data
                po_items = data.get('d', {}).get('results', [])

                # Flatten and clean PO items
                flattened_data = []

                for po in po_items:
                    
                    ebeln = po.get("Ebeln")  # PO Number
                    
                    item_set = po.get("PO_itemSet", {}).get("results", [])
                    
                    
                    for item in item_set:
                        
                        item.pop("__metadata", None)
                        

                        # Convert SAP delivery date to readable format
                        raw_date = item.get("Eindt")
                        
                        readable_date = None
                        

                        if raw_date and raw_date.startswith("/Date("):
                            
                            try:
                                timestamp = int(raw_date.strip("/Date()").split("+")[0]) // 1000  # milliseconds → seconds
                                readable_date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
                            except Exception:
                                readable_date = None

                        # Build cleaned dictionary
                        cleaned_item = {
                            "sap_po_number": ebeln,
                            "sap_po_line_item": item.get("Ebelp"),
                            "sap_batch_number": item.get("Charg"),
                            "sap_material_code": item.get("Matnr"),
                            "sap_material_description": item.get("Maktx"),
                            "sap_ordered_quantity": item.get("Menge"),
                            "sap_uom": item.get("Meins"),
                            "sap_net_price": item.get("Netpr"),
                            "sap_delivery_date": readable_date
                        }

                        flattened_data.append(cleaned_item)

            # Print final output
                return flattened_data
        def flatten_header_details_if_present(data):
            if (
                isinstance(data, dict)
                and len(data) == 1
                and isinstance(list(data.values())[0], dict)
                and "header_details" in list(data.values())[0]
            ):
                return list(data.values())[0]["header_details"]
            return data
            
    
        def process_pdf(file_path):
            try:
                print(f"Processing PDF: {file_path}")

                material_master_data = fetch_sap_master_data()
                sap_data=fetch_sap_data()
                # print(sap_data)
                
                
                print("Completed")

                try:
                    elements = partition_pdf(filename=file_path, languages=["eng"])
                    
                    extracted_text = "\n".join(str(element) for element in elements)
                    # Convert the document
                    # converter = DocumentConverter()
                    # result = converter.convert(file_path)
                    # doc_dict = result.document.export_to_dict()

                    # # Collect all visible text
                    # text_lines = []

                    # def collect_texts(items, key="text"):
                    #     for item in items:
                    #         text = item.get(key)
                    
                    # collect_texts(doc_dict.get("texts", []))

                    # # Extract from table cells
                    # for table in doc_dict.get("tables", []):
                    #     for cell in table.get("data", {}).get("table_cells", []):
                    #         text = cell.get("text")
                    #         if text:
                    #             text_lines.append(text.strip())

                    # # Extract from captions, footnotes, annotations in tables
                    # for section in ["captions", "footnotes", "annotations"]:
                    #     for table in doc_dict.get("tables", []):
                    #         for item in table.get(section, []):
                    #             collect_texts([item])

                    # # Extract from pictures
                    # for pic in doc_dict.get("pictures", []):
                    #     for key in ["captions", "footnotes", "annotations"]:
                    #         for item in pic.get(key, []):
                    #             text = item.get("text")
                    #             if text:
                    #                 text_lines.append(text.strip())

                    # # Extract from form_items and key_value_items
                    # collect_texts(doc_dict.get("form_items", []))
                    # collect_texts(doc_dict.get("key_value_items", []))

                    # # Combine into single plain text
                    # extracted_text = "\n".join(text_lines)

                    # # Final plain text output
                    # print("PLAIN TEXT OUTPUT:\n")
                    # print(extracted_text)      
                    # if text:
                    #         text_lines.append(text.strip())

# 
                    
                    extracted_text_lower = (extracted_text or "").lower()
                    print("Extracted_text_lower",extracted_text_lower)
                    
                    # Step 0: Classify email type using Gemini
                    classification_prompt = f"""
                    You are an AI assistant helping classify emails related to purchasing and logistics. Based on the content provided below — which can include subject, email body, and extracted attachment text — classify the email into one of the following categories:

                    - Bill of Lading (BOL)
                    - Order Confirmation
                    - Generic conversation regarding PO number
                    - Other

                    Definitions:
                    1. **Bill of Lading (BOL)**: Emails related to delivery documents, shipping, dispatch, or tracking — even if the exact term "BOL" is not used.
                    2. **Order Confirmation**: Includes purchase order confirmation, acknowledgement, booking confirmation, or vendor responses that imply the order is accepted.
                    3. **Generic conversation regarding PO number**: Discussion of PO details without explicit confirmation or shipment (e.g., queries, clarifications).
                    4. **Other**: Irrelevant, automated, or non-business content.

                    Please analyze the content semantically. Then respond in the following JSON format:

                    ```json
                    {{
                    "classification": "Order Confirmation",
                    "confidence": 0.87
                    }}

                    Body: {extracted_text_lower}
                    """


                    try:
                            label_response = generate_response_balanced(classification_prompt)
                            if not label_response or not label_response.strip():
                                print("Gemini returned an empty response.")
                                return "Other", 0.0

                            if hasattr(label_response, "text"):
                                raw_response = label_response.text.strip()
                            elif isinstance(label_response, str):
                                raw_response = label_response.strip()
                            else:
                                print("Unexpected response format:", response)
                                raw_response = "{}"
                            # print("Attachment Classified email as::", raw_response)

                            cleaned_response = re.sub(r"^```json\n|\n```$", "", raw_response).strip()
                            result = json.loads(cleaned_response)

                            classification = result.get("classification", "Other")
                            confidence = float(result.get("confidence", 0.0))

                            print(f"Attachment classified email as: {classification} with confidence: {confidence}")
                            label = classification
                    except Exception as e:
                            print(f"Gemini classification error: {e}")
                            label = "Other"

                        # Skip generic or irrelevant emails early
                    if label in ["Generic Email", "Other"]:
                            print("Skipping extraction for non-actionable email.")
                            return "{}"
                    
                    matched_materials = []
                    matched_codes_set = set()

                    lines = extracted_text.splitlines()
                    extracted_text_lower = extracted_text.lower()

                    for entry in material_master_data:
                        material_code = str(entry.get("Material", "")).strip()
                        description = str(entry.get("Description", "")).strip()
                        description_lower = description.lower()

                        for line in lines:
                            line_lower = line.lower()

                            # 1️Direct match by material code
                            if material_code and material_code in line:
                                if material_code not in matched_codes_set:
                                    matched_materials.append({
                                        "material_code": material_code,
                                        "material_description": description,
                                        "matched_on": "material_code"
                                    })
                                    matched_codes_set.add(material_code)

                            # 2️Match by description and material code in same line
                            elif description_lower in line_lower and material_code in line:
                                if material_code not in matched_codes_set:
                                    matched_materials.append({
                                        "material_code": material_code,
                                        "material_description": description,
                                        "matched_on": "description+code in same line"
                                    })
                                    matched_codes_set.add(material_code)

                            # 3️Description matched, code NOT present — infer from master
                            elif description_lower in line_lower and material_code not in line:
                                if material_code not in matched_codes_set:
                                    matched_materials.append({
                                        "material_code": material_code,
                                        "material_description": description,
                                        "matched_on": "description_only_inferred_code"
                                    })
                                    matched_codes_set.add(material_code)

                    # Build context
                    if matched_materials:
                        material_context = "\n### Verified Matches from Master Data:\n"
                        for item in matched_materials:
                            material_context += f"- Matched on **{item['matched_on']}** → Material Code: `{item['material_code']}`, Description: \"{item['material_description']}\"\n"
                    else:
                        material_context = "\n### No Verified Matches Found in Master Data.\n"
                    if "bill of lading" in label.strip().lower():
                        print("Found 'Bill of Lading' or 'BOL' →marking as BOL")
                       
                        print("Started Extraction")
                    
                        # print("system_prompt1")
                        system_prompt = f"""
                        You are an AI assistant. From the text below, extract all PO numbers and their associated CPN codes (which represent material codes).

                        Instructions:
                        - Only extract PO numbers that are explicitly visible in the text and start with "45".
                        - Only extract CPN codes (material codes) that appear with a PO number in the same section or context.
                        - A CPN code always appears in the format "CPN # CODE". Extract only the code portion (e.g., "0016" from "CPN # 0016").
                        - If the text contains "CPN #" but no actual code is shown after it, return "N/A" as the material_code.
                        - If multiple CPNs are found under the same PO, include all.
                        - Do NOT guess or infer any codes.
                        - Ignore unrelated numbers (e.g., skid numbers, PS numbers, BOL numbers, invoice numbers).

                        Output:
                        Respond only in strict JSON format mapping PO numbers to a list of dictionaries of material codes.

                        Example:
                        {{
                        "4500000001": [
                            {{"material_code": "0016"}},
                            {{"material_code": "0022"}}
                        ],
                        "4500000002": [
                            {{"material_code": "0033"}},
                            {{"material_code": "N/A"}}
                        ]
                        }}

                        Extracted Text:
                        {extracted_text_lower}
                        """
                       
                        user_prompt = f"""
                        Extract the PO numbers and their associated material codes from the following Bill of Lading (BOL) text.

                        Instructions:
                        - PO numbers must begin with "45" and be explicitly visible.
                        - Material codes are labeled as "CPN # XXXX" — extract only the numeric part (e.g., "0018").
                        - If the text contains "CPN #" but the actual code is missing or blank, return "material_code": "N/A".
                        - Group material codes under their corresponding PO number based on their nearby or related section in the text.
                        - Multiple material codes per PO are allowed.
                        - Do not guess, infer, or transform values. Use only what is explicitly shown in the text.

                        ### Extracted Text:
                        {extracted_text}

                        ### Output Format (strict JSON only):
                        {{
                        "4500000001": [
                            {{"material_code": "CODE1"}},
                            {{"material_code": "CODE2"}}
                        ],
                        "4500000002": [
                            {{"material_code": "CODE3"}},
                            {{"material_code": "N/A"}}
                        ]
                        }}

                        Return only valid JSON. Do not include any explanations, comments, or extra text.
                        """
                        print("user_prompt1")     
                        prompt = system_prompt + user_prompt
                        match_response = generate_response_balanced(prompt)
                        print("Extraction Completed")
                        if hasattr(match_response, "text"):
                            match_text = match_response.text.strip()
                        elif isinstance(match_response, str):
                            match_text = match_response.strip()
                        else:
                            print("Unexpected Gemini response format:", match_response)
                            match_text = ""
                        # match_text = match_response.text.strip() if match_response and match_response.text else ""
                        print("Gemini PO-material match response1:", match_text)
                        cleaned_match=match_response
                        cleaned_match = re.sub(r"^```(?:json)?\s*|\s*```$", "", match_text.strip(), flags=re.MULTILINE)
                        print("cleaned_match1",cleaned_match)
                        matched_result = json.loads(cleaned_match)
                        print("matched_result",matched_result)
                        cleaned_result = {}
                        reasons = {}
                        bol_records = []

                        for po, items in matched_result.items():
                            valid_codes = {
                                item["sap_material_code"].lstrip("0").strip()
                                for item in sap_data
                                if item.get("sap_material_code") and item.get("sap_po_number") == po
                            }

                            if not valid_codes:
                                reasons[po] = "PO number not found in SAP data."
                                continue

                            print(f"PO from Gemini: {po}")
                            print(f"Valid SAP material codes for PO {po}: {valid_codes}")

                            cleaned_materials = []
                            for entry in items:
                                raw_code = entry.get("material_code", "").strip()
                                extracted = re.split(r"[\\s^]", raw_code)[0].lstrip("0").strip()

                                print(f"Extracted from Gemini: '{raw_code}' → Normalized: '{extracted}'")
                                if raw_code == "N/A":
                                    cleaned_materials.append({
                                        "material_code": "N/A",
                                        "reason": "material code not found"
                                    })
                                elif extracted in valid_codes:
                                    cleaned_materials.append({"material_code": raw_code})
                                else:
                                    reasons[f"{po}:{raw_code}"] = f"Material code '{raw_code}' not valid for PO {po}"
                                # if extracted in valid_codes:
                                #     cleaned_materials.append({"material_code": raw_code})
                                # else:
                                #     reasons[f"{po}:{raw_code}"] = f"Material code '{raw_code}' not valid for PO {po}"

                            if cleaned_materials:
                                cleaned_result[po] = cleaned_materials
                            elif po not in reasons:
                                reasons[po] = "No valid material codes matched for this PO."

                        if not cleaned_result:
                            print("No valid PO-material matches after filtering.")
                            print("Failure reasons:", json.dumps(reasons, indent=2))
                        print(cleaned_result)
                        return cleaned_result

                    
                    else:

                           system_prompt = f"""
                            
                           Extract structured data from the following purchase order. The output must be in clean, valid JSON format with the following structure and rules:

                          
                           Note:
                            - If po_number is not found at the header level, check inside line items.
                            - If multiple PO numbers exist across line items, extract and group data separately for each unique po_number.
                            - Final output must be a **single JSON object** where each key is a unique `po_number` and the value is the corresponding PO data.

                            Example output format:

                            ```json
                            {{
                            "4500586926": {{
                                "po_number": "4500586926",
                                "po_date": "YYYY-MM-DD",
                                "buyer_info": {{...}},
                                "vendor_info": {{...}},
                                "ship_to_address": {{...}},
                                "bill_to": {{ "email": null }},
                                "line_items": [ ... ],
                                "extra_fields": {{...}}
                            }},
                            "4500586927": {{
                                ...
                            }}
                            }}
                           

                            1. Top-level fields:
                            - po_number
                            - po_date 
                            

                            2. Dates:
                            - delivery_date, pickup_date, ship_date . 
                            → If mentioned at header level, assign to top-level.
                            → If part of a line item, assign within that line item only.

                            3. Buyer info:
                            - name
                            - address: street, city, state, zip, country

                            4. Vendor info:
                            - name
                            - address: street, city, state, zip, country

                            5. Ship-to address:
                            - name
                            - address: street, city, state, zip, country

                            6. Bill-to address:
                            - address: street, city, state, zip, country

                            7. Line items: 
                            For each line item extract:
                            - line_number (e.g., 2800028100, or 00010, or 1)
                            - material_code (e.g.,Material #, CUST #: 0113 → material_code = 0113,or 0016)
                            - material_description
                            - quantity (as a single string like "100 BAG", or "4,050.000 LB", or "601 CAR" )
                            - net_price (e.g., "$ 20.00 / 100LB", or "10.12 CAR", or "$ 36.6700 BAG",or "36.6700" or "7.21 LB" or "960" or "10.00") 
                            - total_price (e.g., "$ 3,667.00" or "3,667.00" or $20.00)
                            - delivery_date / pickup_date / ship_date (only if within that row)
                            - Capture any additional metadata (e.g., Net Wgt etc) not fitting inside each line item.
                            
                            Example of line item extraction:(Strictly)
                            Example formats for your internal reference only (not part of extracted text)
                             "line_items": [
                                    {{
                                        "line_number": "00010",
                                        "material_code": "0016",
                                        "material_description": "DEXTROSE",
                                        "quantity": "1,100.000 LB",
                                        "net_price": "$ 20.00 / 100 LB",
                                        "total_price": "$ 240.00",
                                        "delivery_date": "2025-07-22",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }},
                                    {{
                                        "line_number": "00020",
                                        "material_code": "0018",
                                        "material_description": "SWEET DAIRY WHEY-1",
                                        "quantity": "1,000.000 LB",
                                        "net_price": "$ 15.00 / 1 LB",
                                        "total_price": "$ 16,500.00",
                                        "delivery_date": "2025-08-21",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }},
                                    {{
                                        "line_number": "00030",
                                        "material_code": "0020",
                                        "material_description": "GUAR GUM",
                                        "quantity": "250.000 LB",
                                        "net_price": "$ 80.00 / 1 LB",
                                        "total_price": "$ 40,000.00",
                                        "delivery_date": "2025-07-24",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }}
                                    ]
                                                                
                            Notes:
                            -Keep quantity as a single string, do not split into value and unit.
                            - If unit_price is quoted per batch (e.g., "$20.00 / 100 LB"), divide quantity by batch size before multiplying.
                            
                            8. Material Code Resolution Logic (Highly Critical):

                            - First, check if `material_code` is directly mentioned in the document (e.g., labeled as `Material #` or appears near line items).
                            - If not found, look for a `Customer Material Code` or `CUST #` field near the item. Use that as the `material_code`.
                            - If neither is available, attempt to match the `material_description` against the provided **material master list**:
                            - If a match is found on description, use the corresponding `material_code` from the master.
                            - Mark the field `matched_on` as:
                                - `"material_code"` for direct match
                                - `"cust_code"` for customer material code match
                                - `"description_only_inferred_code"` for inferred match via description
                            - If no match is found by any of the above methods, assign `material_code: null`.
                          
                            9. Rules for line items:(High Critical)
                            - `Item #`,`Item Number` → line_number
                            - `Material #`,`CUST #` → material_code
                            - If a code like "234290-xxxxx RDG" appears, extract "234290" as material_code and treat the rest as material_description.
                            - Do not allow `line_number` and `material_code` to be the same; if same, set material_code as null.
                            - Group fields correctly per line item: All fields like `material_code`, `material_description`, `quantity`, `net_price`, `total_price`, and `delivery_date` must belong to the same `line_number`.
                            - Do not split one item across multiple line items or duplicate line numbers unless explicitly shown that way in the document.
                            - Maintain proper sequence of line numbers: e.g., "00010", "00020", "00030".

                            10. Enforcement Rule for total_price:
                            - For each line item, extract total_price only if the value appears in the same visual row (same horizontal line or clearly grouped block) as the line_number and material_description.
                            - Do not copy or shift total_price from the next or previous line item, even if the format is broken.
                            - If total_price is missing in the same visual group, set "total_price": "N/A".
                            - Never borrow from nearby values, subtotals, or grand totals — each total_price must be explicitly and visually tied to that line item only.
                            - If document looks like this:

                                00010 | DEXTROSE        | $20.00
                                00020 | WHEY            |
                                00030 | GUAR GUM        | $4,000.00
                                
                                - Output must be:

                                Example formats for your internal reference only (not part of extracted text)
                                 "line_items": [
                                    {{
                                        "line_number": "00010",
                                        "material_code": "0016",
                                        "material_description": "DEXTROSE",
                                        "quantity": "1,100.000 LB",
                                        "net_price": "$ 20.00 / 100 LB",
                                        "total_price": "$ 20.00",
                                        "delivery_date": "2025-07-22",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }},
                                    {{
                                        "line_number": "00020",
                                        "material_code": "0018",
                                        "material_description": "WHEY",
                                        "quantity": "1,000.000 LB",
                                        "net_price": "$ 15.00 / 1 LB",
                                        "total_price": "N/A",
                                        "delivery_date": "2025-08-21",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }},
                                    {{
                                        "line_number": "00030",
                                        "material_code": "0020",
                                        "material_description": "GUAR GUM",
                                        "quantity": "250.000 LB",
                                        "net_price": "$ 80.00 / 1 LB",
                                        "total_price": "$ 4,000.00",
                                        "delivery_date": "2025-07-24",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }}
                                    ]

                                - Do not let "$4,000.00" slide up to line 00020.
                                - Preserve accuracy even if the layout is irregular.
                            
                            11. Enforcement Rule for quantity:
                            - Only extract `quantity` if the value is in the **same horizontal row** or **same block** as the corresponding `line_number` and `material_description`. If `quantity` is not clearly present on that line, even if other fields are, you must assign `"quantity": "N/A"`. Do not guess or infer it from context or neighboring lines.
                            - If the value appears in a **different visual row**, **under another line item**, or is not **clearly aligned** with the same line item context, **do not extract it**.
                            - In such cases, assign `"N/A"` to avoid incorrect mapping.
                            - Do not assume or infer — **never borrow values** from other line items.

                            12. net_price Handling :
                            - Always extract the net_price exactly as it appears in the document (e.g., $7.21 LB) without modifying or reformatting it (do not convert it to $7.21 / LB or similar).
                            
                            Example of net_price extraction:(Strictly)
                            Example formats for your internal reference only (not part of extracted text)  
                            "line_items": [
                                    {{
                                        "line_number": "1",
                                        "material_code": "214319",
                                        "material_description": "UB-3549 NAT BUTTER GRIDDLE - TY DB KOSHER",
                                        "quantity": "1,100.000 LB",
                                        "net_price": "$7.21 LB",
                                        "total_price": "$ 29,200.50",
                                        "delivery_date": "2025-07-22",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }}
                                    ]

                            13. Visual Alignment Enforcement for All Line Item Fields
                                - For any field (line_number, material_code, material_description, quantity, net_price, total_price, etc.), extract the value only if it appears on the same visual row or block as the other fields of that line item.

                                - If a field is missing from that same row or block, assign "N/A" for that field.

                                - Do not assume or infer values based on the presence of nearby labels, units, or context from other line items.

                                - If a field like "LB", "KG", or "CAR" appears without its corresponding quantity, store the unit in extra_fields, but set "quantity": "N/A".

                                - Never borrow or shift values from the next or previous line item.

                                - This applies to all fields: quantity, net_price, total_price, delivery_date, etc.        
                            
                            - Example of Misaligned line item missing field:(Strictly)
                            Example formats for your internal reference only (not part of extracted text)

                            "line_items": [
                            {{
                                "line_number": "00010",
                                "material_code": "00201",
                                "material_description": "Bulk Corn Flour",
                                "quantity": "N/A",
                                "net_price": "$1.00",
                                "total_price": "$10.00",
                                "delivery_date": "2025-06-20",
                                "pickup_date": null,
                                "ship_date": null,
                                "extra_fields": {{
                                "unit": "LB"
                                }}
                            }}
                            ]
                                    
                            14. Missing Field Handling (Highly Critical):
                            Missing field rules (critical):
                            
                            - If a **label is present** but the value is missing, set the value as `"N/A"` (e.g., `"quantity": "N/A"`).
                            - If a **label is not found at all** in the document, assign the field as `null`.
                            - If a field like `quantity` is **expected** based on the line format but is **not visually present in the same row**, assign `"N/A"`.

                            These must be followed consistently for all fields including buyer_info, addresses, quantities, prices, emails, etc.

                            Do not default to null when "N/A" is required, or vice versa..

                            Example for missing field:
                            {{
                            "line_number": "00050",
                            "material_code": "0090",
                            "material_description": "Guar",
                            "quantity": N/A,
                            "net_price": "$ 10.00 / 1 LB",
                            "total_price": "$ 3,000.00",
                            "delivery_date": "2025-05-04",
                            "pickup_date": null,
                            "ship_date": null,
                            "extra_fields": {{}}
                           }}
                                

                            15. extra_fields:
                            - Capture any additional metadata (e.g., Sales Order, Freight ,po_total_excluding_tax) not fitting standard fields.

                            Ensure:
                            - Valid JSON
                            - No field nesting under `metadata`
                            - No inference or value duplication across sections
                            - Preserve structure across diverse document formats
                            
                            Do not wrap top-level fields (like po_number, po_date, etc.) inside a metadata block.
                            
                            Strictly ensure: Do not repeat line numbers for different products, and always pair the correct `total_price` with the corresponding product line.
                            
                           
                            Document text:
                            {extracted_text_lower}
                            
                            SAP master data:
                            {material_context}

                            """
                           
                           user_prompt = f"""
                            Extract all key fields from the following purchase order including metadata, buyer/vendor info, line items, delivery dates, addresses, and terms. Return the data in a structured JSON format. Here's the input:
                           
                            {extracted_text_lower}
                             """
                 
                    
                    prompt=system_prompt + user_prompt
                    response = generate_response_balanced(prompt)
                    if hasattr(response, "text"):
                        attach_data = response.text.strip()
                    elif isinstance(response, str):
                        attach_data = response.strip()
                    else:
                        print("Unexpected response format:", response)
                        attach_data = "{}"
                    
                    
                    # print("attachment_info1", attach_data)
                    time.sleep(5)
                    return attach_data

                except Exception as e:
                    print(f"Error processing PDF {file_path}: {e}")
                    return {}
                
            except Exception as e:
                print(f"Error processing PDF {file_path}: {e}")
                return {}
    
    

   
        
        def process_excel(file_path):
            print(f"Processing Excel File: {file_path}")
            material_master_data = fetch_sap_master_data()
            sap_data=fetch_sap_data()
            elements = partition_xlsx(filename=file_path)
            # print(elements)
            extracted_text = "\n".join(str(element) for element in elements)
                        
            extracted_text_lower = (extracted_text or "").lower()
            print("Extracted_text_lower",extracted_text_lower)
            
            classification_prompt = f"""
                
                You are an expert document classification assistant specializing in procurement and logistics workflows.

                Your task is to read the content below — extracted from an email attachment or body — and classify it into **one of the following categories**:

                ---

                ### Classification Categories:

                1. **Order Confirmation**
                - Includes: purchase order number, delivery date, item descriptions, item codes, quantities, prices, or vendor details
                - Represents: confirmation or acknowledgment of a purchase order

                2. **Bill of Lading (BOL)**
                - Includes: trailer number, seal number, load date, driver signature, shipment weight, mode of transport, dispatch or tracking information
                - Represents: a physical shipment or dispatch document

                3. **Generic Conversation regarding PO number**
                - Includes: PO number references, general discussion, or queries about orders, but no clear confirmation or shipment information

                4. **Other**
                - Includes: irrelevant, automatic, or incomplete content not related to POs

                ---

                ### Classification Instructions:

                - Prioritize semantic structure, not just keyword occurrence.
                - Do **NOT** classify a document as **BOL** just because it mentions:
                - `"trailer #"`, `"seal #"`, or `"load weight"` alone
                - BOL classification **requires multiple transport-related fields + dispatch intent**

                - If the document contains:
                - `purchase order #`, `delivery date`, `item code`, `quantity`, and product descriptions
                → It is most likely an **Order Confirmation**, even if trailer/seal terms appear.

                - Only choose **BOL** if the main purpose is physical shipment, and **no PO items** or confirmations are present.

                ---

                Please analyze the content semantically. Then respond in the following JSON format:

                ```json
                {{
                "classification": "Order Confirmation",
                "confidence": 0.87
                }}

                Body: {extracted_text_lower}      
            """   
        
            
            
            try:
                label_response = generate_response_balanced(classification_prompt)

                # Step 1: Extract text from response
                if hasattr(label_response, "text"):
                    raw_response = label_response.text.strip()
                elif isinstance(label_response, str):
                    raw_response = label_response.strip()
                else:
                    print("Unexpected response format:", label_response)
                    raw_response = ""

                # Step 2: Check if empty
                if not raw_response:
                    print("Gemini returned an empty response.")
                    return "Other", 0.0

                # Step 3: Clean JSON code block if wrapped in ```json
                cleaned_response = re.sub(r"^```json\n|\n```$", "", raw_response).strip()

                # Step 4: Parse JSON safely
                result = json.loads(cleaned_response)

                classification = result.get("classification", "Other")
                confidence = float(result.get("confidence", 0.0))
                print(f"Classified as: {classification} with confidence: {confidence}")
                label = classification

            except json.JSONDecodeError as e:
                print(f"JSON decode error from Gemini: {e}")
                print("Raw response:", raw_response)
                return "Other", 0.0
            except Exception as e:
                print(f"Gemini classification error: {e}")
                return "Other", 0.0
            
            
            
                        
            matched_materials = []
            matched_codes_set = set()

            for entry in material_master_data:
                material_code = str(entry.get("Material", "")).strip()
                description = str(entry.get("Description", "")).strip()
                description_lower = (description or "").lower()
                

                if material_code and material_code in extracted_text:
                    if material_code not in matched_codes_set:
                        matched_materials.append({
                            "material_code": material_code,
                            "material_description": description,
                            "matched_on": "material_code"
                        })
                        matched_codes_set.add(material_code)
                        

                elif description and description_lower in extracted_text_lower:
                    if material_code not in matched_codes_set:
                        matched_materials.append({
                            "material_code": material_code,
                            "material_description": description,
                            "matched_on": "material_description"
                        })
                        matched_codes_set.add(material_code)
                    

            if matched_materials:
                material_context = "\n### Verified Matches from Master Data:\n"
                for item in matched_materials:
                    material_context += f"- Matched on **{item['matched_on']}** → Material Code: `{item['material_code']}`, Description: \"{item['material_description']}\"\n"
            else:
                material_context = "\n### No Verified Matches Found in Master Data.\n"

            if "bill of lading" in label.strip().lower():
                print("Found 'Bill of Lading' or 'BOL' →marking as BOL")
                        
                print("Started Extraction")
                        
                system_prompt = f"""
                    You are an AI assistant. From the text below, extract all PO numbers and their associated CPN codes (which represent material codes).

                    Instructions:
                    - Only extract PO numbers that are explicitly visible in the text and start with "45".
                    - Only extract CPN codes (material codes) that appear with a PO number in the same section or context.
                    - A CPN code always appears in the format "CPN # CODE". Extract only the code portion (e.g., "0016" from "CPN # 0016").
                    - If the text contains "CPN #" but no actual code is shown after it, return "N/A" as the material_code.
                    - If multiple CPNs are found under the same PO, include all.
                    - Do NOT guess or infer any codes.
                    - Ignore unrelated numbers (e.g., skid numbers, PS numbers, BOL numbers, invoice numbers).

                    Output:
                    Respond only in strict JSON format mapping PO numbers to a list of dictionaries of material codes.

                    Example:
                    {{
                    "4500000001": [
                        {{"material_code": "0016"}},
                        {{"material_code": "0022"}}
                    ],
                    "4500000002": [
                        {{"material_code": "0033"}},
                        {{"material_code": "N/A"}}
                    ]
                    }}

                    Extracted Text:
                    {extracted_text_lower}
                    """
                        
                user_prompt = f"""
                    Extract the PO numbers and their associated material codes from the following Bill of Lading (BOL) text.

                    Instructions:
                    - PO numbers must begin with "45" and be explicitly visible.
                    - Material codes are labeled as "CPN # XXXX" — extract only the numeric part (e.g., "0018").
                    - If the text contains "CPN #" but the actual code is missing or blank, return "material_code": "N/A".
                    - Group material codes under their corresponding PO number based on their nearby or related section in the text.
                    - Multiple material codes per PO are allowed.
                    - Do not guess, infer, or transform values. Use only what is explicitly shown in the text.

                    ### Extracted Text:
                    {extracted_text}

                    ### Output Format (strict JSON only):
                    {{
                    "4500000001": [
                        {{"material_code": "CODE1"}},
                        {{"material_code": "CODE2"}}
                    ],
                    "4500000002": [
                        {{"material_code": "CODE3"}},
                        {{"material_code": "N/A"}}
                    ]
                    }}

                    Return only valid JSON. Do not include any explanations, comments, or extra text.
                    """
                print("user_prompt1")     
                prompt = system_prompt + user_prompt
                match_response = generate_response_balanced(prompt)
                print("Extraction Completed")
                if hasattr(match_response, "text"):
                    match_text = match_response.text.strip()
                elif isinstance(match_response, str):
                    match_text = match_response.strip()
                else:
                    print("Unexpected Gemini response format:", match_response)
                    match_text = ""
                # match_text = match_response.text.strip() if match_response and match_response.text else ""
                print("Gemini PO-material match response1:", match_text)
                cleaned_match=match_response
                cleaned_match = re.sub(r"^```(?:json)?\s*|\s*```$", "", match_text.strip(), flags=re.MULTILINE)
                print("cleaned_match1",cleaned_match)
                matched_result = json.loads(cleaned_match)
                print("matched_result",matched_result)
                cleaned_result = {}
                reasons = {}
                bol_records = []

                for po, items in matched_result.items():
                    valid_codes = {
                        item["sap_material_code"].lstrip("0").strip()
                        for item in sap_data
                        if item.get("sap_material_code") and item.get("sap_po_number") == po
                    }

                    if not valid_codes:
                        reasons[po] = "PO number not found in SAP data."
                        continue

                    print(f"PO from Gemini: {po}")
                    print(f"Valid SAP material codes for PO {po}: {valid_codes}")

                    cleaned_materials = []
                    for entry in items:
                        raw_code = entry.get("material_code", "").strip()
                        extracted = re.split(r"[\\s^]", raw_code)[0].lstrip("0").strip()

                        print(f"Extracted from Gemini: '{raw_code}' → Normalized: '{extracted}'")
                        if raw_code == "N/A":
                            cleaned_materials.append({
                                "material_code": "N/A",
                                "reason": "material code not found"
                            })
                        elif extracted in valid_codes:
                            cleaned_materials.append({"material_code": raw_code})
                        else:
                            reasons[f"{po}:{raw_code}"] = f"Material code '{raw_code}' not valid for PO {po}"
                        # if extracted in valid_codes:
                        #     cleaned_materials.append({"material_code": raw_code})
                        # else:
                        #     reasons[f"{po}:{raw_code}"] = f"Material code '{raw_code}' not valid for PO {po}"

                    if cleaned_materials:
                        cleaned_result[po] = cleaned_materials
                    elif po not in reasons:
                        reasons[po] = "No valid material codes matched for this PO."

                if not cleaned_result:
                    print("No valid PO-material matches after filtering.")
                    print("Failure reasons:", json.dumps(reasons, indent=2))
                print(cleaned_result)
                return cleaned_result

            else:

            
                system_prompt = """
                    You are an expert AI assistant trained to extract structured data from Order Confirmation documents in Excel or PDF formats.

                    ---

                    ### Your Task:

                    Extract **all available fields** from the document — including PO-level metadata, line items, and any extra fields visibly present (such as `carrier`, `vendor_name`, `load weight`, etc.).

                    This document contains only **one confirmed line item**, which must match the following:

                    - `material_code`: 0704
                    - `material_description`: Non-fat Buttermilk -- Lbs.
                    - `quantity`: 40,500 lbs
                    - `net_price`: null (not visibly available)
                    

                    ---
                    ### Special Rule for Quantity:

                    - Do **NOT** treat "totes ordered" or "15 totes" as the quantity.
                    - The correct quantity is: **"40,500 lbs"** — it may appear at the end of the line, with or without the label "total"
                    - Return the full visible value including unit (e.g., "40,500 lbs")
                    ---

                    ### Output Format (Flat JSON):

                    Return a dictionary with these fields:

                    - `po_number`
                    - `po_date`
                    - `delivery_date`
                    - `carrier`
                    - `vendor_name`
                    - Any other extra visible fields (e.g. `load_weight`, `seal_number`, etc.)

                    And a `line_items` list:

                    - Must include only one entry for the confirmed item above
                    - All fields must match what is **visibly available**

                    ---

                    ### Field Extraction Rules:

                    - If a **label is visible** but **value is missing** → return `"N/A"`
                    - If a **label is not present at all** → return `null`
                    - Never guess or infer values from layout or repetition
                    - Do not split line items across rows
                    - Only extract content that is clearly present in the document

                    ---

                    ### Output Constraints:

                    - Return only valid JSON
                    - Do NOT wrap output using PO number as dictionary key
                    - Do NOT include explanations or markdown
                    """
                user_prompt = f"""Extract structured Order Confirmation data from the following text.

                    ---

                    ### Raw Text:
                    {extracted_text}

                    ---

                    ### Output Format (Strict JSON):
                    ```json
                    {{
                    "po_number": "...",
                    "po_date": "...",
                    "delivery_date": "...",
                    "carrier": "...",
                    "vendor_name": "...",
                    "line_items": [
                        {{
                        "material_code": "...",
                        "material_description": "...",
                        "quantity": "...",
                        "net_price": "...",
                        "total_price": "...",
                        "delivery_date": "...",
                        "line_number": ...
                        }}
                    ]
                    }}
                    """
                                        
                prompt=system_prompt + user_prompt
                response = generate_response_balanced(prompt)
                if hasattr(response, "text"):
                    attach_data = response.text.strip()
                elif isinstance(response, str):
                    attach_data = response.strip()
                else:
                    print("Unexpected response format:", response)
                    attach_data = "{}"
                
                
                # print("attachment_info1", attach_data)
                time.sleep(5)
                return attach_data
        

        def process_document(file_path):
            print(f"Processing Document: {file_path}")

            file_ext1 = os.path.splitext(file_path or "")[1].lower()
            if file_ext1 == ".docx":
                elements = partition_docx(filename=file_path)
                extracted_text = "\n".join(str(element) for element in elements)

                # Initialize Gemini Model


                            # Define the system prompt for structured extraction
                system_prompt = f"""You are an AI specialist in processing multi-page Purchase Order (PO) confirmation documents.
                            Your task is to analyze the provided PO confirmation PDF and extract structured data in JSON format while ensuring accuracy and consistency.

                            ### **Instructions:**
                            Mandotory - it will mandortory
                            1. **Extract Key Fields:**
                            - **Purchase Order Number (PO Number):** Identify and extract the PO number from the document.
                            - **Line Items:** Extract quantity, material code, description, item_number (Item #), vendor_material_number (Vendor Mat #) and prices.
                            - **Other Data:** Extract and retain all relevant order details.

                            1.1 **Billing Address Extraction:**
                            - Look for labels such as "Bill To", "Billing Address", "Bill To Address" or similar variations.
                            - Extract the entire address block associated with these labels, maintaining the exact format.
                            - Ensure this is stored under "billing_address" in the JSON output.
                            - Compare `"bill_to"` and `"billing_address"` values. If they are identical, **retain only one entry under the key `"bill_to"`** and **omit the duplicate entry** from the output.
                            - Ensure there are **no duplicate billing address entries** in the final JSON output.

                            1.2 **Special Handling for Item Number and Vendor Material Number:**
                            - Ensure "item_number" and "vendor_material_number" are extracted correctly.
                            - If you encounter the words "Item #", "item_number", "Material #", "Vendor Mat #", "Vendor Material Number", treat them as relevant fields.
                            - Ensure "item_number" and "vendor_material_number" are extracted under their respective keys.
                            - Ensure "description" is not mistakenly treated as "item_number" or "vendor_material_number".

                            1.3 **Material Code Handling (Important):**
                            - If `"material_code"` is present in the input but not extracted properly, ensure it is accurately captured under the `"material_code"` key.
                            - If the `"material_code"` cannot be identified, **do not output it as Null**. Instead, attempt to match it accurately from the input text.
                            - Review surrounding text or labels to accurately extract the `"material_code"`.
                            - **Also, consider `"Material #"`, `"Mat #"` as `"material_code"` and extract them appropriately.**
                            - If `"material_code"`  or `"Material #"` is present in the input but not properly extracted, attempt to reprocess the text to accurately identify and extract it.

                            1.4 **Standardizing Field Names for Consistency:**
                            - Ensure different variations of field names are correctly mapped to their respective standardized keys in the output JSON.
                            - **Material Number:** If any of the following terms appear, consider them as `"material_number"`:
                            `"Material Number"`, `"Item Number"`, `"Customer Material No."`, `"Cust.Material#"`, `"Product No"`, `"Material #"`, `"PART CODE"`.
                            - **Purchase Order:** If any of the following terms appear, consider them as `"purchase_order"`:
                            `"purchase order"`, `"P. O. No"`, `"Purchase Order No."`, `"Purchase Order"`, `"Customer PO No"`, `"Customer PO#"`, `"PO Number"`.
                            - **Quantity:** If any of the following terms appear, consider them as `"quantity"`:
                            `"Quantity"`, `"ordered"`, `"Qty Open"`, `"Qty(UOM)"`, `"Order Qty"`, `"Qty. Ordered"`.
                            - Ensure extracted values are correctly assigned to these standardized keys.

                            1.5 **Ensuring Empty Fields Remain Empty:**
                            - If a field in a line item is **empty in the input**, ensure it remains **empty ("")** in the output JSON instead of inheriting values from adjacent fields.
                            - Prevent cases where missing values (e.g., `"material_code"`, `"quantity"`, `"unit_price"`) incorrectly take values from nearby fields such as `"quantity"` or `"unit_price"`.
                            - If a value is **not present in the input**, explicitly set it as an **empty string ("")** instead of assigning incorrect values from other fields.
                            - This ensures data integrity and avoids incorrect mapping of extracted fields.

                            1.6 **Handling Multi-Page PDFs Without Unnecessary Empty Line Items:**
                            - Ensure that **line items are extracted only when they are present on the page**.
                            - If a PDF has **multiple pages**, and a page **does not contain line items**, do **not** include empty line item fields in the output JSON.
                            - Avoid generating an empty line item structure when no actual line item data exists on a given page.
                            - This ensures that only valid and relevant line item information appears in the output.

                            1.7 **Field Name Standardization & Missing Value Handling:**
                            - Consider the following field name variations as their respective standardized field names:
                                - **Material Number** → Material Number, Item Number, Customer Material No., Cust.Material#, Product No, Material #, PART CODE, material code
                                - **Purchase Order** → P. O. No, Purchase Order No., Purchase Order, Customer PO No, Customer PO#, PO Number
                                - **Quantity** → ordered, Qty Open, Qty(UOM), Order Qty, Qty. Ordered, Quantity
                                - **line_number** → Line#, Total Line Items, Ln, LINE NO
                                - **net_price** → Price, Price($), Unit Price, Price Per Unit, PRICE, net price

                            - **Handling Missing Values:**
                                - If any of the following fields appear in the document (**Purchase Order, Material Number, Quantity, Batch, line_number, net_price**) but their values are missing, set them to **"N/A"** in the output JSON.
                                - If any of these fields do **not** appear in the document at all, set them to **Null** in the output JSON.

                            1.8 **Date Field Standardization:**
                                - Consider the following field name variations as their respective standardized field names:
                                    - **po_date** → Date, Purchase Order Date, Order Date, PO Date
                                    - **delivery_date** → Due Date, Delivery Date, Exp Del Date, Expected Delivery Date

                            1.9 **Material Code Placement:**
                                - Ensure that `material_code` appears **only inside the `line_items` section** in the output JSON.
                                - Do not include `material_code` outside of `line_items`.

                            2. **Ensure Consistency in JSON Format:**
                            - `"po_number"` should store the PO number.
                            - `"line_items"` should contain each item as an object:
                                - `"line_number"`
                                - `"material_code"`
                                - `"description"`
                                - `"quantity"`
                                - `"net_price"`
                                - `"total_price"`

                            3. **Multi-Page Handling:**
                            - If a PO number appears on multiple pages, treat all line items as part of the **same PO**.
                            - If a new PO number appears on another page, create a **separate JSON entry**.

                            4.- Ensure you don't miss any details or any Field('its mandatory)
                            5. Extra field will place after po number and before line item in between them.
                            6.Ensure that:
                                - **All fields maintain their original format**, including **units, currency symbols, and special characters**.
                                - **Dates** should be extracted exactly as they appear (e.g., "April 11, 2025" or "11/04/2025").
                                - **Currency values** should include the symbol (e.g., "₹1,20,000", "$500").
                                - **Quantities should include units** (e.g., "792 BX" instead of just "792").
                                - **Any additional details** appearing in the document should be extracted **exactly as shown**.

                            7. **Line Item Handling (Important & Avoiding Duplicates):**
                                - Capture all line item fields: `item_number`, `vendor_material_number`, `material_code`, `description`, `quantity`, `unit_price`, `total_price`.
                                - Continue adding details to the **same line item** until a new `material_code` or `description` is encountered.
                                - If `material_code` or `description` changes, start a **new line item** entry.
                                - Ensure line items are grouped correctly even if they span across multiple pages.
                                - **Do not return duplicated line items.**
                                - **Ensure each line item entry is unique and appears only once in the output JSON.**
                                - If the same line item is detected more than once, retain only the first occurrence and discard duplicates.

                            8. **Quantity Handling (Critical):**
                                - If the `quantity` field is missing or not found in the document, **do not generate a random number or unit**.
                                - Instead, represent the `quantity` field as `"quantity": Null` in the JSON output.
                                - Ensure that when `quantity` is missing, it is explicitly indicated as `Null` and not replaced with any placeholder value or incorrect data.


                            9. **Description Handling (Critical):**
                                - Any text or information appearing under the `description` field should **ONLY be captured as `description`**.
                                - Under no circumstances should the content of `description` be mistakenly classified as `material_code`, `vendor_material_number`, or `item_number`.
                                - Ensure that the `description` is accurately captured and preserved even if it spans multiple lines or pages.

                            {extracted_text}


                        5.Format the JSON with the following structure:
                            ```json

                            [
                                {{
                                    "po_number": "[PO Number1]",
                                    "vendor_name": "[Vendor Name]",
                                    "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                                    "shipping_address": "[Ship Name] [Ship Adress] [Ship Email] [Phone Number]",
                                    "billing_address": "[Bill Name] [Bill Adress] [Bill Email] [Phone Number]",
                                    "currency": [Currency],
                                    "po_date": "[DD/MM/YYYY]",
                                    "delivery_date": "[DD/MM/YYYY]",
                                    "sub_total": "[Sub Total]",
                                    "total_amount": "[Total Amount]",
                                    "net_weight": "[Net Weight]",
                                    "total_quantity": "[Total Quantity]",
                                    "total_uom": "[Total UOM]",
                                    "total_gross_weight": "[Total Gross Weight]",
                                    "notes": "[Notes]",
                                    "Batch": "[Batch]",
                                    "line_items": [
                                        {{
                                            "line_number": "[Line Number1]",
                                            "item_number": "[Item Number1]",
                                            "vendor_material_number": "[Vendor Material Number1]",
                                            "material_code": "[Material Code1]",
                                            "description": "[Description1]",
                                            "quantity": "[Quantity] [UOM]",
                                            "net_price": "[Net Price]",
                                            "total_price": "[Total Price]"
                                        }},
                                        {{
                                            "line_number": "[Line Number2]",
                                            "item_number": "[Item Number2]",
                                            "vendor_material_number": "[Vendor Material Number2]",
                                            "material_code": "[Material Code2]",
                                            "description": "[Description2]",
                                            "quantity": "[Quantity] [UOM]",
                                            "net_price": "[Net Price]",
                                            "total_price": "[Total Price]"
                                        }}
                                    ],

                                }},
                                {{
                                    "po_number": "[PO Number2]",
                                    "vendor_name": "[Vendor Name]",
                                    "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                                    "shipping_address": "[Ship Name] [Ship Adress] [Ship Email] [Phone Number]",
                                    "billing_address": "[Bill Name] [Bill Adress] [Bill Email] [Phone Number]",
                                    "currency": [Currency],
                                    "po_date": "[DD/MM/YYYY]",
                                    "delivery_date": "[DD/MM/YYYY]",
                                    "sub_total": "[Sub Total]",
                                    "total_amount": "[Total Amount]",
                                    "net_weight": "[Net Weight]",
                                    "total_quantity": "[Total Quantity]",
                                    "total_uom": "[Total UOM]",
                                    "total_gross_weight": "[Total Gross Weight]",
                                    "notes": "[Notes]",
                                    "Batch": "[Batch]",
                                    "line_items": [
                                        {{
                                            "line_number": "[Line Number3]",
                                            "item_number": "[Item Number]",
                                            "vendor_material_number": "[Vendor Material Number]",
                                            "material_code": "[Material Code]",
                                            "description": "[Description]",
                                            "quantity": "[Quantity] [UOM]",
                                            "net_price": "[Net Price]",
                                            "total_price": "[Total Price]"

                                        }}
                                    ],

                                }}
                            ]


                            ```
                            """

                        # User prompt containing extracted text
                user_prompt = f"""Extract Purchase Order details from the following document and return structured JSON.


                            """

                            # Send request to Gemini
                response = generate_response_balanced(system_prompt + user_prompt)
                if hasattr(response, "text"):
                    attach_data = response.text.strip()
                elif isinstance(response, str):
                    attach_data = response.strip()
                else:
                    print("Unexpected response format:", response)
                    attach_data = "{}"


            elif file_ext1 == ".doc":
                elements = partition_doc(filename=file_path)
                print(elements)
                extracted_text = "\n".join(str(element) for element in elements)

            # Initialize Gemini Model


            

                        # Define the system prompt for structured extraction
            system_prompt = f"""You are an AI specialist in processing multi-page Purchase Order (PO) confirmation documents.
                        Your task is to analyze the provided PO confirmation PDF and extract structured data in JSON format while ensuring accuracy and consistency.

                        ### **Instructions:**
                        Mandotory - it will mandortory
                        1. **Extract Key Fields:**
                        - **Purchase Order Number (PO Number):** Identify and extract the PO number from the document.
                        - **Line Items:** Extract quantity, material code, description, item_number (Item #), vendor_material_number (Vendor Mat #) and prices.
                        - **Other Data:** Extract and retain all relevant order details.
                        1.1 **Billing Address Extraction:**
                        - Look for labels such as "Bill To", "Billing Address", "Bill To Address" or similar variations.
                        - Extract the entire address block associated with these labels, maintaining the exact format.
                        - Ensure this is stored under "billing_address" in the JSON output.
                        - Compare `"bill_to"` and `"billing_address"` values. If they are identical, **retain only one entry under the key `"bill_to"`** and **omit the duplicate entry** from the output.
                        - Ensure there are **no duplicate billing address entries** in the final JSON output.

                        1.2 **Special Handling for Item Number and Vendor Material Number:**
                        - Ensure "item_number" and "vendor_material_number" are extracted correctly.
                        - If you encounter the words "Item #", "item_number", "Material #", "Vendor Mat #", "Vendor Material Number", treat them as relevant fields.
                        - Ensure "item_number" and "vendor_material_number" are extracted under their respective keys.
                        - Ensure "description" is not mistakenly treated as "item_number" or "vendor_material_number".

                        1.3 **Material Code Handling (Important):**
                        - If `"material_code"` is present in the input but not extracted properly, ensure it is accurately captured under the `"material_code"` key.
                        - If the `"material_code"` cannot be identified, **do not output it as Null**. Instead, attempt to match it accurately from the input text.
                        - Review surrounding text or labels to accurately extract the `"material_code"`.
                        - **Also, consider `"Material #"`, `"Mat #"` as `"material_code"` and extract them appropriately.**
                        - If `"material_code"`  or `"Material #"` is present in the input but not properly extracted, attempt to reprocess the text to accurately identify and extract it.

                        1.4 **Standardizing Field Names for Consistency:**
                        - Ensure different variations of field names are correctly mapped to their respective standardized keys in the output JSON.
                        - **Material Number:** If any of the following terms appear, consider them as `"material_number"`:
                        `"Material Number"`, `"Item Number"`, `"Customer Material No."`, `"Cust.Material#"`, `"Product No"`, `"Material #"`, `"PART CODE"`.
                        - **Purchase Order:** If any of the following terms appear, consider them as `"purchase_order"`:
                        `"purchase order"`, `"P. O. No"`, `"Purchase Order No."`, `"Purchase Order"`, `"Customer PO No"`, `"Customer PO#"`, `"PO Number"`.
                        - **Quantity:** If any of the following terms appear, consider them as `"quantity"`:
                        `"Quantity"`, `"ordered"`, `"Qty Open"`, `"Qty(UOM)"`, `"Order Qty"`, `"Qty. Ordered"`.
                        - Ensure extracted values are correctly assigned to these standardized keys.

                        1.5 **Ensuring Empty Fields Remain Empty:**
                        - If a field in a line item is **empty in the input**, ensure it remains **empty ("")** in the output JSON instead of inheriting values from adjacent fields.
                        - Prevent cases where missing values (e.g., `"material_code"`, `"quantity"`, `"unit_price"`) incorrectly take values from nearby fields such as `"quantity"` or `"unit_price"`.
                        - If a value is **not present in the input**, explicitly set it as an **empty string ("")** instead of assigning incorrect values from other fields.
                        - This ensures data integrity and avoids incorrect mapping of extracted fields.

                        1.6 **Handling Multi-Page PDFs Without Unnecessary Empty Line Items:**
                        - Ensure that **line items are extracted only when they are present on the page**.
                        - If a PDF has **multiple pages**, and a page **does not contain line items**, do **not** include empty line item fields in the output JSON.
                        - Avoid generating an empty line item structure when no actual line item data exists on a given page.
                        - This ensures that only valid and relevant line item information appears in the output.

                        1.7 **Field Name Standardization & Missing Value Handling:**
                        - Consider the following field name variations as their respective standardized field names:
                            - **Material Number** → Material Number, Item Number, Customer Material No., Cust.Material#, Product No, Material #, PART CODE, material code
                            - **Purchase Order** → P. O. No, Purchase Order No., Purchase Order, Customer PO No, Customer PO#, PO Number
                            - **Quantity** → ordered, Qty Open, Qty(UOM), Order Qty, Qty. Ordered, Quantity
                            - **line_number** → Line#, Total Line Items, Ln, LINE NO
                            - **net_price** → Price, Price($), Unit Price, Price Per Unit, PRICE, net price

                        - **Handling Missing Values:**
                            - If any of the following fields appear in the document (**Purchase Order, Material Number, Quantity, Batch, line_number, net_price**) but their values are missing, set them to **"N/A"** in the output JSON.
                            - If any of these fields do **not** appear in the document at all, set them to **Null** in the output JSON.

                        1.8 **Date Field Standardization:**
                            - Consider the following field name variations as their respective standardized field names:
                                - **po_date** → Date, Purchase Order Date, Order Date, PO Date
                                - **delivery_date** → Due Date, Delivery Date, Exp Del Date, Expected Delivery Date

                        1.9 **Material Code Placement:**
                            - Ensure that `material_code` appears **only inside the `line_items` section** in the output JSON.
                            - Do not include `material_code` outside of `line_items`.

                        2. **Ensure Consistency in JSON Format:**
                        - `"po_number"` should store the PO number.
                        - `"line_items"` should contain each item as an object:
                            - `"line_number"`
                            - `"material_code"`
                            - `"description"`
                            - `"quantity"`
                            - `"net_price"`
                            - `"total_price"`

                        3. **Multi-Page Handling:**
                        - If a PO number appears on multiple pages, treat all line items as part of the **same PO**.
                        - If a new PO number appears on another page, create a **separate JSON entry**.

                        4.- Ensure you don't miss any details or any Field('its mandatory)
                        5. Extra field will place after po number and before line item in between them.
                        6.Ensure that:
                            - **All fields maintain their original format**, including **units, currency symbols, and special characters**.
                            - **Dates** should be extracted exactly as they appear (e.g., "April 11, 2025" or "11/04/2025").
                            - **Currency values** should include the symbol (e.g., "₹1,20,000", "$500").
                            - **Quantities should include units** (e.g., "792 BX" instead of just "792").
                            - **Any additional details** appearing in the document should be extracted **exactly as shown**.
                            - Ensure "item_number" and "vendor_material_number" are extracted correctly.
                        7. **Line Item Handling (Important):**
                            - Capture all line item fields: `item_number`, `vendor_material_number`, `material_code`, `description`, `quantity`, `unit_price`, `total_price`.
                            - Continue adding details to the **same line item** until a new `material_code` or `description` is encountered.
                            - If `material_code` or `description` changes, start a **new line item** entry.
                            - Ensure line items are grouped correctly even if they span across multiple pages.
                            - **Do not return duplicated line items.**
                            - **Ensure each line item entry is unique and appears only once in the output JSON.**
                            - If the same line item is detected more than once, retain only the first occurrence and discard duplicates.

                        8. **Quantity Handling (Critical):**
                        - If the `quantity` field is missing or not found in the document, **do not generate a random number or unit**.
                        - Instead, represent the `quantity` field as `"quantity": Null` in the JSON output.
                        - Ensure that when `quantity` is missing, it is explicitly indicated as `Null` and not replaced with any placeholder value or incorrect data.

                        9. **Description Handling (Critical):**
                            - Any text or information appearing under the `description` field should **ONLY be captured as `description`**.
                            - Under no circumstances should the content of `description` be mistakenly classified as `material_code`, `vendor_material_number`, or `item_number`.
                            - Ensure that the `description` is accurately captured and preserved even if it spans multiple lines or pages.

                        {extracted_text}


                    5.Format the JSON with the following structure:
                        ```json

                        [
                            {{
                                "po_number": "[PO Number1]",
                                "vendor_name": "[Vendor Name]",
                                "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                                "shipping_address": "[Ship Name] [Ship Adress] [Ship Email] [Phone Number]",
                                "billing_address": "[Bill Name] [Bill Adress] [Bill Email] [Phone Number]",
                                "currency": [Currency],
                                "po_date": "[DD/MM/YYYY]",
                                "delivery_date": "[DD/MM/YYYY]",
                                "sub_total": "[Sub Total]",
                                "total_amount": "[Total Amount]",
                                "net_weight": "[Net Weight]",
                                "total_quantity": "[Total Quantity]",
                                "total_uom": "[Total UOM]",
                                "total_gross_weight": "[Total Gross Weight]",
                                "notes": "[Notes]",
                                "Batch": "[Batch]",
                                "line_items": [
                                    {{
                                        "line_number": "[Line Number1]",
                                        "item_number": "[Item Number1]",
                                        "vendor_material_number": "[Vendor Material Number1]",
                                        "material_code": "[Material Code1]",
                                        "description": "[Description1]",
                                        "quantity": "[Quantity] [UOM]",
                                        "net_price": "[Net Price]",
                                        "total_price": "[Total Price]"
                                    }},
                                    {{
                                        "line_number": "[Line Number2]",
                                        "item_number": "[Item Number2]",
                                        "vendor_material_number": "[Vendor Material Number2]",
                                        "material_code": "[Material Code2]",
                                        "description": "[Description2]",
                                        "quantity": "[Quantity] [UOM]",
                                        "net_price": "[Net Price]",
                                        "total_price": "[Total Price]"
                                    }}
                                ],

                            }},
                            {{
                                "po_number": "[PO Number2]",
                                "vendor_name": "[Vendor Name2]",
                                "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                                "shipping_address": "[Ship Name] [Ship Adress] [Ship Email] [Phone Number]",
                                "billing_address": "[Bill Name] [Bill Adress] [Bill Email] [Phone Number]",
                                "currency": [Currency],
                                "po_date": "[DD/MM/YYYY]",
                                "delivery_date": "[DD/MM/YYYY]",
                                "sub_total": "[Sub Total]",
                                "total_amount": "[Total Amount]",
                                "net_weight": "[Net Weight]",
                                "total_quantity": "[Total Quantity]",
                                "total_uom": "[Total UOM]",
                                "total_gross_weight": "[Total Gross Weight]",
                                "notes": "[Notes]",
                                "Batch": "[Batch]",
                                "line_items": [
                                    {{
                                        "line_number": "[Line Number]",
                                        "item_number": "[Item Number]",
                                        "vendor_material_number": "[Vendor Material Number]",
                                        "material_code": "[Material Code]",
                                        "description": "[Description]",
                                        "quantity": "[Quantity] [UOM]",
                                        "net_price": "[Net Price]",
                                        "total_price": "[Total Price]"
                                    }}
                                ],

                            }}
                        ]


                        ```
                        """

                        # User prompt containing extracted text
            user_prompt = f"""Extract Purchase Order details from the following document and return structured JSON.


                        """

                        # Send request to Gemini
            response = generate_response_balanced(system_prompt + user_prompt)
            if hasattr(response, "text"):
                attach_data = response.text.strip()
            elif isinstance(response, str):
                attach_data = response.strip()
            else:
                print("Unexpected response format:", response)
                attach_data = "{}"
                
                
            return attach_data    


        def process_image(file_path):
            print(f"Processing Image: {file_path}")
            try:
                if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                    print("Image file is empty or missing.")
                    return "{}"  # Return empty JSON-like object
                with open(file_path, "rb") as f:
                    img_bytes = f.read()

                # Use PIL to check if it's a valid image
                img = Image.open(io.BytesIO(img_bytes))
                img.verify()  # Ensures image integrity

            # Applies the English and Swedish language pack for ocr
                elements = partition_image(filename=io.BytesIO(img_bytes),languages=["eng"],strategy="ocr_only")
                print(elements)

                extracted_text = "\n".join(str(element) for element in elements)



                if not extracted_text:
                    print("No text extracted from the image.")
                    return {}

                print("Extracted Text from Image:")
                print(extracted_text)


            

                    # Define the system prompt for structured extraction
                system_prompt = f"""# Define the instruction prompt
                        Convert the following extracted raw data into a structured JSON format. Follow these rules strictly:

                        1. The `po_number` must be the first key in the JSON.
                        2. Extract the `po_number` **only** from the **Customer Purchase Order** field.
                        - Consider `Customer Purchase Order`, even if it appears in different cases (e.g., 'customer purchase order', 'CUSTOMER PURCHASE ORDER').
                        - **Do NOT** consider `Order Number`, `Customer Number`, or any similar term as `po_number`.
                        - If `Customer Purchase Order` is not found, set `"po_number": "Not Available"`.
                        3. No extracted data should be skipped or removed—retain every field and value.
                        4. Extra fields (metadata, dates, vendor details, etc.) must appear after `po_number`.
                        5. Line items should be placed inside a `"line_items"` array at the end of the JSON.
                        6. Preserve all text, numbers, special characters, and original formatting exactly as extracted.
                        7. **Field Name Standardization & Missing Value Handling:**
                        - Consider the following field name variations as their respective standardized field names:
                            - **Material Number** → Material Number, Item Number, Customer Material No., Cust.Material#, Product No, Material #, PART CODE, material code
                            - **Purchase Order** → P. O. No, Purchase Order No., Purchase Order, Customer PO No, Customer PO#, PO Number
                            - **Quantity** → ordered, Qty Open, Qty(UOM), Order Qty, Qty. Ordered, Quantity
                            - **line_number** → Line#, Total Line Items, Ln, LINE NO
                            - **net_price** → Price, Price($), Unit Price, Price Per Unit, PRICE, net price

                        - **Handling Missing Values:**
                            - If any of the following fields appear in the document (**Purchase Order, Material Number, Quantity, Batch, line_number, net_price**) but their values are missing, set them to **"N/A"** in the output JSON.
                            - If any of these fields do **not** appear in the document at all, set them to **Null** in the output JSON.

                        8. Enforcement Rule for total_price:
                            - For each line item, extract total_price only if the value appears in the same visual row (same horizontal line or clearly grouped block) as the line_number and material_description.
                            - Do not copy or shift total_price from the next or previous line item, even if the format is broken.
                            - If total_price is missing in the same visual group, set "total_price": "N/A".
                            - Never borrow from nearby values, subtotals, or grand totals — each total_price must be explicitly and visually tied to that line item only.
                            - If document looks like this:

                                00010 | DEXTROSE        | $20.00
                                00020 | WHEY            |
                                00030 | GUAR GUM        | $4,000.00
                                
                                - Output must be:

                                Example formats for your internal reference only (not part of extracted text)
                                 "line_items": [
                                    {{
                                        "line_number": "00010",
                                        "material_code": "0016",
                                        "material_description": "DEXTROSE",
                                        "quantity": "1,100.000 LB",
                                        "net_price": "$ 20.00 / 100 LB",
                                        "total_price": "$ 20.00",
                                        "delivery_date": "2025-07-22",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }},
                                    {{
                                        "line_number": "00020",
                                        "material_code": "0018",
                                        "material_description": "WHEY",
                                        "quantity": "1,000.000 LB",
                                        "net_price": "$ 15.00 / 1 LB",
                                        "total_price": "N/A",
                                        "delivery_date": "2025-08-21",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }},
                                    {{
                                        "line_number": "00030",
                                        "material_code": "0020",
                                        "material_description": "GUAR GUM",
                                        "quantity": "250.000 LB",
                                        "net_price": "$ 80.00 / 1 LB",
                                        "total_price": "$ 4,000.00",
                                        "delivery_date": "2025-07-24",
                                        "pickup_date": null,
                                        "ship_date": null,
                                        "extra_fields": {{}}
                                    }}
                                    ]

                                - Do not let "$4,000.00" slide up to line 00020.
                                - Preserve accuracy even if the layout is irregular.

                        9. **Date Field Standardization:**
                            - Consider the following field name variations as their respective standardized field names:
                                - **po_date** → Date, Purchase Order Date, Order Date, PO Date
                                - **delivery_date** → Due Date, Delivery Date, Exp Del Date, Expected Delivery Date

                        10. **Material Code Placement:**
                            - Ensure that `material_code` appears **only inside the `line_items` section** in the output JSON.
                            - Do not include `material_code` outside of `line_items`.

                        ### Raw Extracted Data:
                        {extracted_text}

                        ### Expected JSON Format Example:
                        ```json
                        {{
                        "po_number": "[PO Number]",
                        "vendor_name": "[Vendor Name]",
                        "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                        "po_date": "[DD/MM/YYYY]",
                        "delivery_date": "[DD/MM/YYYY]",
                        "payment_terms": "[Payment Terms]",
                        "currency": "[Currency]",
                        "shipping_address": "[Ship Name] [Ship Adress] [Ship Email] [Phone Number]",
                        "extra_field_1": "[value]",
                        "extra_field_2": "[value]",
                        "line_items": [
                            {{
                            "line_number":"[Line Number]",
                            "material_code": "[Material Code]",
                            "description": "[Description]",
                            "quantity": "[Quantity] [UOM]",
                            "net_price": "[Net Price]",
                            "total_price": "[Total Price]"
                            }}
                        ]
                        }}

                    ```
                    """

                    # User prompt containing extracted text
                user_prompt = f"""Extract Purchase Order details from the following image and return structured JSON.
                """

                    # Send request to Gemini
                response = generate_response_balanced(system_prompt + user_prompt)
                if hasattr(response, "text"):
                    attach_data = response.text.strip()
                elif isinstance(response, str):
                    attach_data = response.strip()
                else:
                    print("Unexpected response format:", response)
                    attach_data = "{}"
                # Extract response text
                extracted_json = attach_data.text.strip() if attach_data and attach_data.text else "{}"

                # Ensure valid JSON output
                extracted_json = re.sub(r"^```json\n|\n```$", "", extracted_json, flags=re.MULTILINE).strip()

                print("Extracted JSON Output:")
                print(extracted_json)
                time.sleep(5)
                return extracted_json


            except Exception as e:
                print(f"Error processing Image {file_path}: {e}")
                return {}


        def process_html(file_path):
            print(f"Processing html: {file_path}")
            try:
                if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                    print("HTML file is empty or missing.")
                    return "{}"  # Return empty JSON-like structure
                with open(file_path, 'r', encoding='utf-8') as file:
                    html_content = file.read()

                # Ensure content is not empty after reading
                if not html_content.strip():
                    print("No content found in the HTML file.")
                    return "{}"



                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                for tag in soup(['script', 'style', 'img','meta','link']):
                    tag.extract()

                

                text_content = soup.get_text(separator='\n', strip=True)
                if not text_content.strip():
                    print("No meaningful text found in HTML.")
                    return "{}"

                elements=[NarrativeText(text_content)]
                # for data in extracted_data:
                #     print(data)

                extracted_text = "\n".join(str(element) for element in elements)



                

                # Define the system prompt for structured extraction
                system_prompt = f"""# Define the instruction prompt
                            Convert the following extracted raw data into a structured JSON format. Follow these rules strictly:

                            1. The `po_number` must be the first key in the JSON.
                            customer  purchase order
                            **Special Handling for the Word "item_number":**
                            - If you encounter the word `"item_no.","Material #"`, treat it as the **material_code** field.
                            - Ensure the **item_no.**,**Material #**  is extracted under the `"material_code"` key.
                            - Ensure Description is not confused with material_code.
                            2. No extracted data should be skipped or removed—retain every field and value.
                            3. Extra fields (metadata, dates, vendor details, etc.) must appear after `po_number`.
                            4. Line items should be placed inside a `"line_items"` array at the end of the JSON.
                            5. Preserve all text, numbers, special characters, and original formatting exactly as extracted.
                            6. **Field Name Standardization & Missing Value Handling:**
                            - Consider the following field name variations as their respective standardized field names:
                                - **Material Number** → Material Number, Item Number, Customer Material No., Cust.Material#, Product No, Material #, PART CODE, material code
                                - **Purchase Order** → P. O. No, Purchase Order No., Purchase Order, Customer PO No, Customer PO#, PO Number
                                - **Quantity** → ordered, Qty Open, Qty(UOM), Order Qty, Qty. Ordered, Quantity
                                - **line_number** → Line#, Total Line Items, Ln, LINE NO
                                - **net_price** → Price, Price($), Unit Price, Price Per Unit, PRICE, net price

                            - **Handling Missing Values:**
                                - If any of the following fields appear in the document (**Purchase Order, Material Number, Quantity, Batch, line_number, net_price**) but their values are missing, set them to **"N/A"** in the output JSON.
                                - If any of these fields do **not** appear in the document at all, set them to **Null** in the output JSON.

                            7. **Date Field Standardization:**
                                - Consider the following field name variations as their respective standardized field names:
                                    - **po_date** → Date, Purchase Order Date, Order Date, PO Date
                                    - **delivery_date** → Due Date, Delivery Date, Exp Del Date, Expected Delivery Date
                            9 **Material Code Placement:**
                                - Ensure that `material_code` appears **only inside the `line_items` section** in the output JSON.
                                - Do not include `material_code` outside of `line_items`.

                            ### Raw Extracted Data:
                            {extracted_text}

                            ### Expected JSON Format Example:
                            ```json
                            {{
                            "po_number": "[PO Number]",
                            "vendor_name": "[Vendor Name]",
                            "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                            "order_date": "[DD/MM/YYYY]",
                            "payment_terms": "[Payment Terms]",
                            "currency": "[Currency]",
                            "shipping_address": "[Ship Name] [Ship Adress] [Ship Email] [Phone Number]",
                            "extra_field_1": "[value]",
                            "extra_field_2": "[value]",
                            "line_items": [
                                {{
                                "line_number":"[Line Number1]",
                                "material_code": "[Material Code1]",
                                "description": "[Description1]",
                                "quantity": "[Quantity] [UOM]",
                                "net_price": "[Net Price]",
                                "total_price": "[Total Price]"
                                }},
                                {{
                                "line_number":"[Line Number2]",
                                "material_code": "[Item Number2]",
                                "description": "[Description2]",
                                "quantity": "[Quantity] [UOM]",
                                "net_price": "[Net Price]",
                                "total_price": "[Total Price]"
                                }}
                            ]
                            }}

                        ```
                        """

                        # User prompt containing extracted text
                user_prompt = f"""Extract Purchase Order details from the following image and return structured JSON.


                        """

                        # Send request to Gemini
                response = generate_response_balanced(system_prompt + user_prompt)
                if hasattr(response, "text"):
                    attach_data = response.text.strip()
                elif isinstance(response, str):
                    attach_data = response.strip()
                else:
                    print("Unexpected response format:", response)
                    attach_data = "{}"

                # Extract response text
                extracted_json = attach_data.text.strip() if attach_data and attach_data.text else "{}"

                # Ensure valid JSON output
                extracted_json = re.sub(r"^```json\n|\n```$", "", extracted_json, flags=re.MULTILINE).strip()

                print("Extracted JSON Output:")
                print(extracted_json)
                time.sleep(5)
                return extracted_json



            except Exception as e:
                print(f"Error processing HTML {file_path}: {e}")
                return "{}"

        def process_txt(file_path):
            print(f"Processing html: {file_path}")
            elements = partition_text(filename=file_path)
            print(elements)
            extracted_text = "\n".join(str(element) for element in elements)


                        # Define the system prompt for structured extraction
            system_prompt = f"""You are an AI specialist in processing multi-page Purchase Order (PO) confirmation documents.
                        Your task is to analyze the provided PO confirmation PDF and extract structured data in JSON format while ensuring accuracy and consistency.

                        ### **Instructions:**
                        Mandotory - it will mandortory
                        1. **Extract Key Fields:**
                        - **Purchase Order Number (PO Number):** Identify and extract the PO number from the document.
                        - **Line Items:** Extract quantity, material code, description, item_number (Item #), vendor_material_number (Vendor Mat #) and prices.
                        - **Other Data:** Extract and retain all relevant order details.
                        1.2 **Field Name Standardization & Missing Value Handling:**
                        - Consider the following field name variations as their respective standardized field names:
                            - **Material Number** → Material Number, Item Number, Customer Material No., Cust.Material#, Product No, Material #, PART CODE, material code
                            - **Purchase Order** → P. O. No, Purchase Order No., Purchase Order, Customer PO No, Customer PO#, PO Number
                            - **Quantity** → ordered, Qty Open, Qty(UOM), Order Qty, Qty. Ordered, Quantity
                            - **line_number** → Line#, Total Line Items, Ln, LINE NO
                            - **net_price** → Price, Price($), Unit Price, Price Per Unit, PRICE, net price

                        - **Handling Missing Values:**
                            - If any of the following fields appear in the document (**Purchase Order, Material Number, Quantity, Batch, line_number, net_price**) but their values are missing, set them to **"N/A"** in the output JSON.
                            - If any of these fields do **not** appear in the document at all, set them to **Null** in the output JSON.

                        1.3 **Date Field Standardization:**
                            - Consider the following field name variations as their respective standardized field names:
                                - **po_date** → Date, Purchase Order Date, Order Date, PO Date
                                - **delivery_date** → Due Date, Delivery Date, Exp Del Date, Expected Delivery Date
                        1.9 **Material Code Placement:**
                            - Ensure that `material_code` appears **only inside the `line_items` section** in the output JSON.
                            - Do not include `material_code` outside of `line_items`.

                        2. **Ensure Consistency in JSON Format:**
                        - `"po_number"` should store the PO number.
                        - `"line_items"` should contain each item as an object:
                            - `"line_number"`
                            - `"material_code"`
                            - `"description"`
                            - `"quantity"`
                            - `"net_price"`
                            - `"total_price"`

                        3. **Multi-Page Handling:**
                        - If a PO number appears on multiple pages, treat all line items as part of the **same PO**.
                        - If a new PO number appears on another page, create a **separate JSON entry**.

                        4.- Ensure you don't miss any details or any Field('its mandatory)
                        5. Extra field will place after po number and before line item in between them.
                        6.Ensure that:
                            - **All fields maintain their original format**, including **units, currency symbols, and special characters**.
                            - **Dates** should be extracted exactly as they appear (e.g., "April 11, 2025" or "11/04/2025").
                            - **Currency values** should include the symbol (e.g., "₹1,20,000", "$500").
                            - **Quantities should include units** (e.g., "792 BX" instead of just "792").
                            - **Any additional details** appearing in the document should be extracted **exactly as shown**.

                        {extracted_text}


                    5.Format the JSON with the following structure:
                        ```json

                        [
                            {{
                                "po_number": "[PO Number1]",
                                "vendor_name": "[Vendor Name]",
                                "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                                "shipping_address": "[Ship Name] [Ship Adress] [Ship Email] [Phone Number]",
                                "billing_address": "[Bill Name] [Bill Adress] [Bill Email] [Phone Number]",
                                "currency": [Currency],
                                "po_date": "[DD/MM/YYYY]",
                                "delivery_date": "[DD/MM/YYYY]",
                                "sub_total": "[Sub Total]",
                                "total_amount": "[Total Amount]",
                                "net_weight": "[Net Weight]",
                                "total_quantity": "[Total Quantity]",
                                "total_uom": "[Total UOM]",
                                "total_gross_weight": "[Total Gross Weight]",
                                "notes": "[Notes]",
                                "Batch": "[Batch]",
                                "line_items": [
                                    {{
                                        "line_number":"[Line Number1]",
                                        "material_code": "[Material Code1]",
                                        "description": "[Description1]",
                                        "quantity": "[Quantity] [UOM]",
                                        "net_price": "[Net Price]",
                                        "total_price": "[Total Price]"
                                    }},
                                    {{
                                        "line_number":"[Line Number2]",
                                        "material_code": "[Material Code2]",
                                        "description": "[Description2]",
                                        "quantity": "[Quantity] [UOM]",
                                        "net_price": "[Net Price]",
                                        "total_price": "[Total Price]"
                                        }}
                                ],

                            }},
                            {{
                                "po_number": "[PO Number2]",
                                "vendor_name": "[Vendor Name]",
                                "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                                "shipping_address": "[Ship Name] [Ship Adress] [Ship Email] [Phone Number]",
                                "billing_address": "[Bill Name] [Bill Adress] [Bill Email] [Phone Number]",
                                "currency": [Currency],
                                "po_date": "[DD/MM/YYYY]",
                                "delivery_date": "[DD/MM/YYYY]",
                                "sub_total": "[Sub Total]",
                                "total_amount": "[Total Amount]",
                                "net_weight": "[Net Weight]",
                                "total_quantity": "[Total Quantity]",
                                "total_uom": "[Total UOM]",
                                "total_gross_weight": "[Total Gross Weight]",
                                "notes": "[Notes]",
                                "Batch": "[Batch]",
                                "line_items": [
                                    {{
                                        "line_number":"[Line Number]",
                                        "material_code": "[Material Code]",
                                        "description": "[Description]",
                                        "quantity": "[Quantity] [UOM]",
                                        "net_price": "[Net Price]",
                                        "total_price": "[Total Price]"
                                    }}
                                ],

                            }}
                        ]


                        ```
                        """

                        # User prompt containing extracted text
            user_prompt = f"""Extract Purchase Order details from the following document and return structured JSON.


                        """

                        # Send request to Gemini
            response = generate_response_balanced(system_prompt + user_prompt)
            if hasattr(response, "text"):
                attach_data = response.text.strip()
            elif isinstance(response, str):
                attach_data = response.strip()
            else:
                print("Unexpected response format:", response)
                attach_data = "{}"
            return attach_data

        time.sleep(5)
    
        def extract_email_subject(email_subject):
            try:
                """Extract PO number and material_code from email subject using Gemini AI and validate with master data."""
                
                material_master_data = fetch_sap_master_data()
                # Step 0: Classify email type using Gemini
                classification_prompt = f"""
                You are an AI assistant helping classify emails related to purchasing and logistics. Based on the content provided below — which can include subject, email body, and extracted attachment text — classify the email into one of the following categories:

                - Bill of Lading (BOL)
                - Order Confirmation
                - Generic conversation regarding PO number
                - Other

                Definitions:
                1. **Bill of Lading (BOL)**: Emails related to delivery documents, shipping, dispatch, or tracking — even if the exact term "BOL" is not used.
                2. **Order Confirmation**: Includes purchase order confirmation, acknowledgement, booking confirmation, or vendor responses that imply the order is accepted.
                3. **Generic conversation regarding PO number**: Discussion of PO details without explicit confirmation or shipment (e.g., queries, clarifications).
                4. **Other**: Irrelevant, automated, or non-business content.

                Please analyze the content semantically. Then respond in the following JSON format:

                ```json
                {{
                "classification": "Order Confirmation",
                "confidence": 0.87
                }}

                Body: {email_subject}
                """


                try:
                        label_response = generate_response_balanced(classification_prompt)
                        if not label_response or not label_response.strip():
                            print("Gemini returned an empty response.")
                            return "Other", 0.0

                        if hasattr(label_response, "text"):
                            raw_response = label_response.text.strip()
                        elif isinstance(label_response, str):
                            raw_response = label_response.strip()
                        else:
                            print("Unexpected response format:", label_response)
                            raw_response = "{}"

                        # print("Subject Classified email as::", raw_response)

                        cleaned_response = re.sub(r"^```json\n|\n```$", "", raw_response).strip()
                        result = json.loads(cleaned_response)

                        classification = result.get("classification", "Other")
                        confidence = float(result.get("confidence", 0.0))

                        print(f"Subject classified email as: {classification} with confidence: {confidence}")
                        label = classification
                except Exception as e:
                        print(f"Gemini classification error: {e}")
                        label = "Other"

                    # Skip generic or irrelevant emails early
                if label in ["Generic Email", "Other"]:
                        print("Skipping extraction for non-actionable email.")
                        return "{}"

                subject_lower = (email_subject or "").lower()
                matched_materials = []
                matched_codes_set = set()  # Avoid duplicates

                for entry in material_master_data:
                        material_code = str(entry.get("Material", "")).strip()
                        description = str(entry.get("Description", "")).strip()
                        description_lower = (description or "").lower()

                        if material_code and material_code in subject_lower:
                            if material_code not in matched_codes_set:
                                matched_materials.append({
                                    "material_code": material_code,
                                    "material_description": description,
                                    "matched_on": "material_code"
                                })
                                matched_codes_set.add(material_code)

                        elif description and description_lower in subject_lower:
                            if material_code not in matched_codes_set:
                                matched_materials.append({
                                    "material_code": material_code,
                                    "material_description": description,
                                    "matched_on": "material_description"
                                })
                                matched_codes_set.add(material_code)

                material_context = "\n### Verified Matches from Master Data:\n"
                if matched_materials:
                    for item in matched_materials:
                        material_context += f"- Matched on **{item['matched_on']}** → material_code: `{item['material_code']}`, material_description: \"{item['material_description']}\"\n"
                else:
                    material_context += "No direct material_code or material_description matches found in the subject.\n"

                prompt = f"""
                    **Instructions:**
                    You are given the subject line of an email. Your task is to extract structured data in strict JSON format.

                    ### Extraction Rules:
                    - Extract only the fields that are clearly present in the subject line.
                    - Use the verified material master data below to confirm material_code or material_description.

                    {material_context}

                    ### Field Standardization:
                    - **material_code** → Material Number, Item Number, Customer Material No., Cust.Material#, Product No, Material #, PART CODE
                    - **purchase_order** → P. O. No, Purchase Order No., Purchase Order, Customer PO No, Customer PO#, PO Number

                    ###Missing Values Handling:
                    - If the **field label is present but value is missing**, return `"N/A"`.
                    - If the **field label is completely absent**, return `null`.

                    ### Output JSON Format:
                    {{
                    "po_number": "[PO Number]",
                    "line_items": [
                        {{
                        "material_code": "[Material Code]",
                        }}
                    ]
                    }}

                    **Email Subject:**
                    {email_subject}

                    **Extracted JSON:**
                    """

                try:
                    response_sub = generate_response_balanced(prompt)
                    if hasattr(response_sub, "text"):
                        extracted_subject_data = response_sub.text.strip()
                    elif isinstance(response_sub, str):
                        extracted_subject_data = response_sub.strip()
                    else:
                        print("⚠️ Unexpected response format:", response_sub)
                        extracted_subject_data = "{}"
                    time.sleep(1)
                    return extracted_subject_data
                except Exception as e:
                    print(f"Error extracting email subject with Gemini: {e}")
                    return "{}"
            except Exception as e:
                print(f"Error extracting email subject with Gemini: {e}")
                return "{}"
    
   
  
        def extract_email_content(email_body):
            """Extract relevant information from an email body using Gemini AI."""
            try:    
                material_master_data = fetch_sap_master_data()
                
                # Step 0: Classify email type using Gemini
                classification_prompt = f"""
                You are an AI assistant helping classify emails related to purchasing and logistics. Based on the content provided below — which can include subject, email body, and extracted attachment text — classify the email into one of the following categories:

                - Bill of Lading (BOL)
                - Order Confirmation
                - Generic conversation regarding PO number
                - Other

                Definitions:
                1. **Bill of Lading (BOL)**: Emails related to delivery documents, shipping, dispatch, or tracking — even if the exact term "BOL" is not used.
                2. **Order Confirmation**: Includes purchase order confirmation, acknowledgement, booking confirmation, or vendor responses that imply the order is accepted.
                3. **Generic conversation regarding PO number**: Discussion of PO details without explicit confirmation or shipment (e.g., queries, clarifications).
                4. **Other**: Irrelevant, automated, or non-business content.

                Please analyze the content semantically. Then respond in the following JSON format:

                ```json
                {{
                "classification": "Order Confirmation",
                "confidence": 0.87
                }}

                Body: {email_body}
                """


                try:
                        label_response = generate_response_balanced(classification_prompt)
                        if not label_response or not label_response.strip():
                            print("Gemini returned an empty response.")
                            return "Other", 0.0

                        if hasattr(label_response, "text"):
                            raw_response = label_response.text.strip()
                        elif isinstance(label_response, str):
                            raw_response = label_response.strip()
                        else:
                            print("Unexpected response format:", label_response)
                            raw_response = "{}"

                        # print("Body Classified email as::", raw_response)

                        cleaned_response = re.sub(r"^```json\n|\n```$", "", raw_response).strip()
                        result = json.loads(cleaned_response)

                        classification = result.get("classification", "Other")
                        confidence = float(result.get("confidence", 0.0))

                        print(f"Body classified email as: {classification} with confidence: {confidence}")
                        label = classification
                except Exception as e:
                        print(f"Gemini classification error: {e}")
                        label = "Other"

                    # Skip generic or irrelevant emails early
                if label in ["Generic Email", "Other"]:
                        print("Skipping extraction for non-actionable email.")
                        return "{}"
                # Step 1: Match values from subject against master data
                body_lower = (email_body or "").lower()
                matched_materials = []
                matched_codes_set = set()  # Avoid duplicates

                for entry in material_master_data:
                        material_code = str(entry.get("Material", "")).strip()
                        description = str(entry.get("Description", "")).strip()
                        description_lower = (description or "").lower()

                        if material_code and material_code in body_lower:
                            if material_code not in matched_codes_set:
                                matched_materials.append({
                                    "material_code": material_code,
                                    "material_description": description,
                                    "matched_on": "material_code"
                                })
                                matched_codes_set.add(material_code)

                        elif description and description_lower in body_lower:
                            if material_code not in matched_codes_set:
                                matched_materials.append({
                                    "material_code": material_code,
                                    "material_description": description,
                                    "matched_on": "material_description"
                                })
                                matched_codes_set.add(material_code)

                material_context = "\n### Verified Matches from Master Data:\n"
                if matched_materials:
                    for item in matched_materials:
                        material_context += f"- Matched on **{item['matched_on']}** → material_code: `{item['material_code']}`, material_description: \"{item['material_description']}\"\n"
                else:
                    material_context += "No direct material_code or material_description matches found in the email body.\n"

 
                system_prompt = f"""
                You are a reliable AI system that extracts structured purchase order data from any email body, including tables and free text.

                Your job is to extract all visible fields in a deterministic and structured JSON format, without skipping any content.

                ---

                Extraction Goals:
                1. Extract all visible fields exactly as shown — do not skip or summarize anything.
                2. Always include these mandatory fields:
                    - "po_number"
                    - "po_date"
                    - "delivery_date"
                    - "material_code"
                    - "material_description"
                    - "quantity"
                    - "net_price"
                    - "total_price"
                    

                3. Strict Missing Field Rules:
                    - If a label is present but the value is missing → return `"N/A"`
                    - If a label is not present → return `null`
                    - Never guess values.

                4. Line Number Extraction(Highly critical):
                    - If a label like “line”, “line number”, “item no.” is present → extract as line_number.
                    - In this case, retain material_code separately as well.
                    - Clean line_number to remove special characters.

                    Example formats for your internal reference only (not part of email body):
                       
                    Input1: 0016 Dextrose #00010 100LB 50.00/100LB 20.00
                    
                    Output1:
                    "line_items":[{{
                    "line_number": "00010",
                    "material_code": "0016",
                    "material_description": "Dextrose",
                    "quantity": "100LB",
                    "net_price": "50.00/1LB",
                    "total_price": "20.00",
                    "batch": null
                    }}]
                    
                    Input2:0018 SWEET DAIRY WHEY-1 #00020 315LB 15.00/1LB 4,000.00
                    
                    Output2:     
                    "line_items":[{{
                    "line_number": "00020",
                    "material_code": "0018",
                    "material_description": "SWEET DAIRY WHEY-1",
                    "quantity": "315LB",
                    "net_price": "15.00/1LB",
                    "total_price": "4,000.00",
                    "batch": null
                    }}]
                    
                    
                    Input3:0020 GUAR GUM #00030 50LB 80.00/1LB 650.00
                    
                    Output3:
                    "line_items":[{{
                    "line_number": "00030",
                    "material_code": "0020",
                    "material_description": "GUAR GUM",
                    "quantity": "50LB",
                    "net_price": "80.00/1LB",
                    "total_price": "650.00",
                    "batch": null
                    }}]
                    

                5. Quantity and Net Price:
                    - If quantity label is missing → `null`
                    - If quantity label present but value missing → `"N/A"`
                    - Same rule for net price.
                    - Never copy quantity from net price, or vice versa.
                    - Quantity must retain units if present (e.g., "100 KG", "792 BX").

                6. Extra Fields:
                    - If any extra information is found (e.g., GTIN, sales order, shipment date, batch, special notes, comments, vendor info):
                    - Store inside a dictionary called `"extra_fields"`.
                    - Extra fields must be properly captured, even if not requested explicitly.

                7. Material Master Matching:
                    - If material code matches → use it.
                    - Else if material description matches → map the correct material code.
                    - If the material code is shown in brackets after a product number (e.g., "130005004 (228412)"), extract the value in brackets as the `material_code`.

                

                8. Grouping and Isolation:
                    - Each line item must be independent.
                    - Never borrow missing fields from adjacent lines.
                    - One line item = one complete set of fields.

                9. General Output Rules:
                    - Output must always be valid strict JSON.
                    - Keep formatting consistent.
                    - Always deterministic: Same input → Same output.

                ---

                ### Verified Material Master Context:
                {material_context}

                ---
  
                ### Email Body Content:
                {email_body}

                ---

                Expected Final JSON Structure:
                ```json
                {{
                    "po_number": "[PO Number]",
                    "vendor_name": "[Vendor Name]",
                    "vendor_details": "[Contact Name] [Contact Email] [Phone Number]",
                    "sales_order": "[Sales Number]",
                    "po_date": "[DD/MM/YYYY]",
                    "shipment_date": "[DD/MM/YYYY]",
                    "delivery_date": "[DD/MM/YYYY]",
                    "extra_fields": {{
                        "batch_number": "[Batch]",
                        "special_notes": "[Notes]",
                        ...
                    }},
                    "line_items": [
                        {{
                            "line_number": "[Line Number]",
                            "material_code": "[Material Code]",
                            "material_description": "[Material Description]",
                            "quantity": "[Quantity] [UOM]",
                            "net_price": "[Currency] [Net Price] [UOM]",
                            "total_price": "[Currency] [Total Price]",
                            "batch": "[Batch]"
                        }}
                    ]
                }}
                """
                user_prompt = f"""Extract Purchase Order details from the following email body and return structured JSON.
                                        "Make sure if a field is present but the value is missing, return 'N/A'. "
                                        "If the field is absent, return null."
                                        (critical)"Do not skip any visible content from the email body".
                                        Extract structured purchase order details from the email body provided.

                                        - If any required field is present but empty, return "N/A"
                                        - If a field is missing entirely, return null
                                        - If line items are missing, return one line item with all mandatory fields as null
                                        - Do not skip any visible text
                                        - Return only valid JSON

                    """

                try:
                    response_body = generate_response_balanced(system_prompt+user_prompt)
                    if hasattr(response_body, "text"):
                        extracted_body_info = response_body.text.strip()
                    elif isinstance(response_body, str):
                        extracted_body_info = response_body.strip()
                    else:
                        print("Unexpected response format:", response_body)
                        extracted_body_info = "{}"
                    # Fix structure right after response
                    # extracted_info = fix_po_structure(extracted_info)
                    time.sleep(1)  # Prevent hitting API rate limits
                    return extracted_body_info
                except Exception as e:
                    print(f"Error extracting email content with Gemini: {e}")
                    return "{}"  # Return empty JSON if extraction fails
            except Exception as e:
                print(f"Error extracting email content with Gemini: {e}")
                return "{}"    
   

        def clean_json_output(json_str):
            try:
                if not json_str or json_str.strip() in ["[]", "{}"]:
                    return {}

                # Remove markdown/code block fencing
                json_str = re.sub(r"^```json\n|\n```$", "", json_str.strip(), flags=re.MULTILINE)

                try:
                    parsed = json.loads(json_str)

                    # If it's a list with 1 dict, return that dict directly
                    if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
                        return parsed[0]

                    # If it's already a dict
                    if isinstance(parsed, dict):
                        return parsed

                    print("Unexpected structure, returning empty dict.")
                    return {}

                except json.JSONDecodeError as e:
                    print("JSON parsing error! Returning empty dictionary.")
                    print(f"Raw JSON: {json_str}")
                    return {}
            except Exception as e:
                print(f"Error cleaning JSON output: {e}")
                return {}    
   
        
     
        def normalize_po_number(po):
            """
            Clean up and normalize a PO number to just digits.
            Removes prefixes like 'PO ', 'po:', 'PO number:', etc.
            """
            try:
                if not po:
                    return None
                po = str(po).strip()
                po = re.sub(r"(?i)^po\\s*[:\\-]*\\s*", "", po)  # remove 'PO', 'PO:', 'PO-', etc.
                po = re.sub(r"(?i)^po number\\s*[:\\-]*\\s*", "", po)
                po = re.sub(r"[^0-9]", "", po)  # remove all non-numeric characters
                return po if po else None
            except Exception as e:
                print(f"Error normalizing PO number: {e}")
                return None
   
        
        
       
        def classify_email(subject=None, body=None, has_attachments=None, email_id=None, access_token=None,
                        received_datetime=None, sender_email=None, base64_eml=None):
            """Classify email based on subject, body, and attachments."""
            try:
                global processing_queue
                global proof_collection
                print("\U0001F680 Starting email classification...")

                subject = subject if subject else None
                body = body if body else None
                attachments_info = []
                base64_attachments = []
                base64_eml = base64_eml if base64_eml else None

                # Step 1: Extract subject/body content
                extracted_subject_info = extract_email_subject(subject)
                extracted_subject_info = re.sub(r"^```json\n|\n```$", "", extracted_subject_info, flags=re.MULTILINE).strip()

                extracted_body_info = extract_email_content(body)
                extracted_body_info = re.sub(r"^```(?:json)?\s*|\s*```$", "", extracted_body_info.strip(), flags=re.MULTILINE)
                print("Extracted_body_info",extracted_body_info)

                # Step 2: Handle attachments
                if has_attachments and email_id and access_token:
                    print("\U0001F4E9 Email has attachments. Downloading...")
                    attachments_list = download_attachments(access_token, email_id)
                    print(f"Attachments downloaded: {attachments_list}")

                    for attachment in attachments_list:
                        base64_data = convert_file_to_base64(attachment)
                        base64_attachments.append({"filename": os.path.basename(attachment), "base64": base64_data})

                        if attachment not in processing_queue and attachment not in processed_files:
                            processing_queue.append(attachment)
                            print(f"Enqueued {attachment}. Queue now: {list(processing_queue)}")

                    if processing_queue:
                        print("\U0001F501 Processing attachments now...")
                        attachments_info = process_attachments()
                        print("Attachments_info:", attachments_info)

                # Step 3: Parse extracted JSON safely
                try:
                    extracted_subject_data = clean_and_parse_json(extracted_subject_info)
                except json.JSONDecodeError:
                    extracted_subject_data = {}

                try:
                    grouped_results1 = {}
                    extracted_body_data = clean_and_parse_json(extracted_body_info)
                    print("Extracted_body_data",extracted_body_data)
                    if isinstance(extracted_body_data, dict):
                        if all(isinstance(v, dict) and "po_number" in v for v in extracted_body_data.values()):
                            for po_num, po_data in extracted_body_data.items():
                                        normalize_line_items(po_data)
                                        grouped_results1[po_num] = po_data
                        else:
                            po = extracted_body_data.get("po_number")
                            if po:
                                grouped_results1[po] = extracted_body_data
                            extracted_body_data = grouped_results1
                
                except json.JSONDecodeError:
                            extracted_body_data = {}

                try:
                    extracted_attachment_data = json.loads(attachments_info) if isinstance(attachments_info, str) else attachments_info
                    print("Extracted_attachment_data1",extracted_attachment_data)
                except json.JSONDecodeError:
                    extracted_attachment_data = {}
                
                
                print("extracted_subject_data",extracted_subject_data)
                print("extracted_body_data",extracted_body_data)
                print("extracted_attachment_data",extracted_attachment_data)
                # Step 4: Merge all available sources
                combined_data = merge_unique_values(extracted_subject_data, extracted_body_data, extracted_attachment_data)
                combined_data, is_asn = clean_asn_json(combined_data)
                print("Combined Data:", combined_data)
                formatted_output = format_json_structure(combined_data)

                print("Final Merged Output:", json.dumps(formatted_output, indent=2))

                if isinstance(formatted_output, dict):
                    for po_number, po_data in formatted_output.items():
                        if not isinstance(po_data, dict):
                            continue

                        clean_po_number = normalize_po_number(po_number)

                        if not clean_po_number:
                            print(f"Skipping: Invalid or missing PO number: {po_number}")
                            continue
                        # Check if this is a BOL
                        if po_data.get("label") == "Bill of Lading":
                            print(f"Proof: Identified BOL for PO {clean_po_number}")

                            # Prepare BOL entries with full metadata
                            bol_entries = []
                            # for item in po_data.get("line_items", []):
                            bol_entry = {
                                    "po_number": clean_po_number,
                                    "received_datetime": received_datetime,
                                    "sender_email": sender_email,
                                    "attachments": base64_attachments if has_attachments else None,
                                    "eml_file": base64_eml,
                                }
                            bol_entries.append(bol_entry)

                            try:
                                existing_proof = proof_collection.find_one({"_id": clean_po_number})
                                if existing_proof:
                                    existing_bol = existing_proof.get("BOL", [])
                                    existing_bol.extend(bol_entries)

                                    # # Optional: Deduplicate by po_number + material_code
                                    # deduped_bol = list({(b["po_number"], b["material_code"]): b for b in existing_bol}.values())

                                    # Only update the BOL field, never touch OC proof at top-level
                                    proof_collection.update_one(
                                        {"_id": po_number},
                                        {"$set": {"BOL": existing_bol}}
                                    )
                                    print(f"BOL proof appended for PO: {clean_po_number}")

                                else:
                                    # No existing OC proof, create full new document with BOL array
                                    new_doc = {
                                        "_id": clean_po_number,
                                        "BOL": bol_entries
                                    }
                                    proof_collection.insert_one(new_doc)
                                    print(f"Inserted BOL proof for new PO: {clean_po_number}")
                       

                            except Exception as e:
                                print(f"Error storing BOL proof for PO {clean_po_number}: {e}")
                        
                        else:
                            # For Order Confirmation (existing logic)
                            proof_data = {
                                "_id": clean_po_number,
                                "received_datetime": received_datetime,
                                "sender_email": sender_email,
                                "attachments": base64_attachments if has_attachments else None,
                                "eml_file": base64_eml
                            }

                            try:
                                proof_collection.insert_one(proof_data)
                                print(f"Proof data stored for PO: {clean_po_number}")
                            except pymongo.errors.DuplicateKeyError:
                                print(f"Duplicate PO Number {clean_po_number}, updating existing record...")
                                proof_collection.update_one({"_id": clean_po_number}, {"$set": proof_data})
              

                        # Validate and store PO
                        validation_status, missing_fields, extracted_status = validate_extracted_data(po_data)
                        print(f"Validation Result → Valid: {validation_status} | Status: {extracted_status} | Missing: {missing_fields}")

                        if (extracted_status or '').lower() in ["confirmed", "completed", "complete detail"]:
                            store_po_in_cosmos(collection, {clean_po_number: po_data}, extracted_status, is_asn=is_asn,received_datetime=received_datetime)
                        elif (extracted_status or '').lower() == "missing":
                            process_and_store_data({clean_po_number: po_data}, extracted_status)

                    clear_attachment_folder()
                    return {"message": "Processed all POs from subject, body, and attachments."}

                return "No PO data available to store."
            except Exception as e:
                print(f"Error classifying email: {e}")

    
     
        def normalize_line_items(po_data):
            try:
                if isinstance(po_data.get("line_items"), dict):
                    line_items_dict = po_data["line_items"]
                    po_data["line_items"] = list(line_items_dict.values())
            except Exception as e:
                print(f"Error normalizing line_items: {e}") 
        
            
      

        def merge_unique_values(extracted_subject_data=None, extracted_body_data=None, extracted_attachment_data=None):
            """Merge multiple dictionaries, preferring valid values over null/'N/A' and handling nested structures."""

            def is_valid(value):
                return value not in [None, "N/A", "", {}]

            def merge_dicts(d1, d2):
                
                    merged = {}
                    for key in set(d1.keys()).union(d2.keys()):
                        v1 = d1.get(key)
                        v2 = d2.get(key)

                        if isinstance(v1, dict) and isinstance(v2, dict):
                            merged[key] = merge_dicts(v1, v2)
                        elif isinstance(v1, dict):
                            merged[key] = v1
                        elif isinstance(v2, dict):
                            merged[key] = v2
                        elif isinstance(v1, list) and isinstance(v2, list):
                            # Merge lists uniquely
                            merged_list = [json.loads(item) for item in {json.dumps(i, sort_keys=True) for i in (v1 + v2)}]
                            # Filter out mostly-empty items (especially for line_items)
                            if key == "line_items":
                                merged[key] = [
                                    item for item in merged_list
                                    if any(is_valid(v) for k, v in item.items() if k != "material_code")
                                    or is_valid(item.get("material_code"))
                                ]
                            else:
                                merged[key] = merged_list
                        elif isinstance(v1, list):
                            merged[key] = v1
                        elif isinstance(v2, list):
                            merged[key] = v2
                        else:
                            # Prefer valid value over None or "N/A"
                            if not is_valid(v1) and is_valid(v2):
                                merged[key] = v2
                            elif not is_valid(v2) and is_valid(v1):
                                merged[key] = v1
                            elif v1 != v2:
                                if is_valid(v1) and is_valid(v2):
                                    merged[key] = v1  # or choose v2 if needed, or keep both in a list
                                elif is_valid(v1):
                                    merged[key] = v1
                                elif is_valid(v2):
                                    merged[key] = v2
                                else:
                                    # both are invalid (e.g. None and "N/A"), pick one
                                    merged[key] = "N/A" if "N/A" in [v1, v2] else None
                            else:
                                merged[key] = v1
                    return merged
            try:
                    merged_data = {}
                    for data in [extracted_subject_data, extracted_body_data, extracted_attachment_data]:
                        if isinstance(data, dict):
                            merged_data = merge_dicts(merged_data, data)
                    # Step 2: Reprioritize po_number (attachment > body > subject)
                    po_from_attachment = (extracted_attachment_data or {}).get("po_number")
                    po_from_body = (extracted_body_data or {}).get("po_number")
                    po_from_subject = (extracted_subject_data or {}).get("po_number")

                    if is_valid(po_from_attachment):
                        merged_data["po_number"] = po_from_attachment
                    elif is_valid(po_from_body):
                        merged_data["po_number"] = po_from_body
                    elif is_valid(po_from_subject):
                        merged_data["po_number"] = po_from_subject
                    else:
                        merged_data["po_number"] = None

                    return merged_data
                
            except Exception as e:
                    print(f"Error merging dictionaries: {e}")
                    return {}
  
        
        def format_json_structure(merged_data):
            """
            Reorders the final merged JSON:
            - Places "po_number" first.
            - Moves "line_items" to the last position.
            - Keeps all other fields in between.
            """
            try:
                if not isinstance(merged_data, dict):
                    return merged_data  # Return as-is if it's not a dictionary

                # Extract and reorder fields
                po_number = merged_data.pop("po_number", None)  # Extract PO number
                line_items = merged_data.pop("line_items", None)  # Extract line items

                # Construct new ordered dictionary
                formatted_json = {}

                if po_number:
                    formatted_json["po_number"] = po_number  # Ensure PO number is first

                # Add all other metadata fields (except line_items)
                for key, value in merged_data.items():
                    formatted_json[key] = value

                # Append "line_items" at the end
                if line_items:
                    formatted_json["line_items"] = line_items  # Ensure line_items is last

                return formatted_json
            except Exception as e:
                print(f"Error formatting JSON structure: {e}")
                return {}
  

        def validate_extracted_data(data):
            """
            Validate PO extraction status based on field presence and content.

            Returns:
            - is_valid: True only if po_number is present and valid
            - flagged_fields: all fields with "N/A" or null
            - status: "Missing", "Confirmed", or "Complete Detail"
            """
            try:
                flagged_fields = {}
                has_missing_fields = False
                has_complete_line = False

                po_number = data.get("po_number")
                if not po_number or po_number == "N/A":
                    return False, {"po_number": po_number or "null"}, None

                line_items = data.get("line_items", [])
                if isinstance(line_items, list) and line_items:
                    for item in line_items:
                        if not isinstance(item, dict):
                            continue

                        required_fields = ["material_code", "quantity", "total_price"]

                        missing_values = [item.get(field) in ["N/A"] for field in required_fields]

                        # Keep the missing_field tag (but don't use it for status logic)
                        item["missing_field"] = True if any(missing_values) else False

                        # If all required fields are null → consider this as "no meaningful data"
                        if all(item.get(field) is None for field in required_fields):
                            continue  # treat as empty, don't affect status

                        # If even one field is "N/A" or null, it's missing
                        if any(missing_values):
                            has_missing_fields = True
                            for field in required_fields:
                                if item.get(field) in ["N/A"]:
                                    flagged_fields[field] = "N/A"
                        else:
                            has_complete_line = True
                else:
                    line_items = []

                # Final status logic
                if has_missing_fields:
                    status = "Missing"
                elif has_complete_line:
                    status = "Complete Detail"
                else:
                    status = "Confirmed"

                return True, flagged_fields, status

            except Exception as e:
                print(f"Error validating extracted data: {e}")
                return False, {}, None

    
        
        def convert_file_to_base64(file_path):
            """Convert a file to Base64 encoding."""
            try:
                if not os.path.exists(file_path):
                    print(f"File does not exist at path: {file_path}")
                    return None

                with open(file_path, "rb") as file:
                    base64_encoded = base64.b64encode(file.read()).decode("utf-8")
                    print("File successfully converted to Base64.")
                    return base64_encoded

            except Exception as e:
                print(f"Error converting file to Base64:\nPath: {file_path}\nError: {e}")
                return None
   
      
    
        def process_and_store_data(extracted_data, extracted_status):
            """Processes and stores PO data. If validation fails, stores it in the missing collection."""
            try:
                # 🔁 If it's a dict of multiple POs, process each one
                if all(isinstance(v, dict) for v in extracted_data.values()):
                    for po_number, po_data in extracted_data.items():
                        process_and_store_data(po_data, extracted_status)
                    return

                validation_status, missing_fields, extracted_status = validate_extracted_data(extracted_data)
                po_number = extracted_data.get("po_number", None)

                if not po_number:
                    print("Cannot store without PO number.")
                    return

                # Bill of Lading logic
                if extracted_data.get("label") == "Bill of Lading":
                    print(f"Processing Bill of Lading for PO {po_number}")
                    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    bol_items = []
                    for item in extracted_data.get("line_items", []):
                        bol_entry = {
                            "po_number": po_number,
                            "material_code": item.get("material_code", "N/A"),
                            "received_datetime": now,
                            "bol_status": False,
                            "missing_field": True if item.get("material_code") == "N/A" else False
                        }
                        if bol_entry["missing_field"] == True:
                            bol_entry["reason"] = "material code not found"
                        bol_items.append(bol_entry)

                    bol_doc = {
                        "_id": po_number,
                        "po_number": po_number,
                        "label": "Bill of Lading",
                        # "received_datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        # "bol_status": False,
                        "BOL": bol_items
                    }

                    # If it's missing, also mark the status
                    if (extracted_status or "").lower() == "missing":
                        bol_doc["status"] = "missing"
                        bol_doc["human_intervention_required"] = True
                        bol_doc["flagged_fields"] = missing_fields

                    # Choose target collection
                    target_collection = collection if collection.find_one({"_id": po_number}) else missing_collection
                    existing_doc = target_collection.find_one({"_id": po_number})

                    if existing_doc:
                        print(f"Appending BOL to existing PO {po_number}")
                        updated_bol = existing_doc.get("BOL", []) + bol_items

                        # Optional: Deduplicate using full dictionary match
                        seen = set()
                        deduped_bol = []
                        for entry in updated_bol:
                            key = json.dumps(entry, sort_keys=True)
                            if key not in seen:
                                seen.add(key)
                                deduped_bol.append(entry)

                        target_collection.update_one(
                            {"_id": po_number},
                            {"$set": {"BOL": updated_bol}}
                        )
                    else:
                        target_collection.insert_one(bol_doc)
                        print(f"New BOL PO inserted: {po_number}")
                    return

                # If missing, store in missing collection
                if (extracted_status or "").lower() == "missing":
                    print(f"Logging missing fields for {po_number}.")

                    missing_entry = dict(extracted_data)
                    missing_entry["_id"] = po_number
                    missing_entry["status"] = "missing"
                    missing_entry["human_intervention_required"] = True
                    missing_entry["flagged_fields"] = missing_fields

                    try:
                        missing_collection.insert_one(missing_entry)
                        print(f"Missing PO stored for manual review: {po_number}")
                    except pymongo.errors.DuplicateKeyError:
                        missing_collection.update_one({"_id": po_number}, {"$set": missing_entry})
                        print(f"Updated missing PO {po_number} in Missing collection.")
                else:
                    # Handle ASN or regular PO
                    shipping_keys = ["carrier_name", "tracking_number", "mode_of_transport", "shipment_date"]
                    shipping_present = any(extracted_data.get(k) not in [None, "", "N/A"] for k in shipping_keys)

                    asn_fields = [
                        "tracking_number", "vendor_name", "po_date", "vendor_details",
                        "shipment_date", "delivery_date", "mode_of_transport", "carrier_name"
                    ]
                    asn_payload = {k: extracted_data.get(k) for k in asn_fields if extracted_data.get(k) not in [None, ""]}

                    existing_doc = collection.find_one({"_id": po_number})

                    if shipping_present and any(asn_payload.values()):
                        print(f"Adding ASN details to PO {po_number}")
                        if existing_doc:
                            asn_detail = existing_doc.get("asn_detail", [])
                            if not isinstance(asn_detail, list):
                                asn_detail = [asn_detail] if asn_detail else []
                            asn_detail.append(asn_payload)
                            collection.update_one({"_id": po_number}, {"$set": {"asn_detail": asn_detail}})
                        else:
                            extracted_data["asn_detail"] = [asn_payload]
                            collection.insert_one({**extracted_data, "_id": po_number})
                            print(f"New PO {po_number} inserted with ASN.")
                    else:
                        if existing_doc:
                            stored_line_items = existing_doc.get("line_items", [])
                            new_line_items = extracted_data.get("line_items", [])

                            stored_copy = {k: v for k, v in existing_doc.items() if k not in ["_id", "line_items", "asn_detail"]}
                            current_copy = {k: v for k, v in extracted_data.items() if k not in ["line_items"]}

                            if stored_copy != current_copy:
                                print(f"PO-level metadata for {po_number} is different. Not appending.")
                            else:
                                combined_line_items = stored_line_items + new_line_items
                                seen = set()
                                unique_line_items = []
                                for item in combined_line_items:
                                    key = json.dumps(item, sort_keys=True)
                                    if key not in seen:
                                        seen.add(key)
                                        unique_line_items.append(item)

                                collection.update_one(
                                    {"_id": po_number},
                                    {"$set": {
                                        **current_copy,
                                        "line_items": unique_line_items
                                    }}
                                )
                                print(f"Updated PO {po_number}: new line items appended.")
                        else:
                            collection.insert_one({**extracted_data, "_id": po_number})
                            print(f"New PO {po_number} inserted.")
            except Exception as e:
                print("[Error25]")
                print(f"Error: {e}\nTraceback:\n{traceback.format_exc()}")
        
  
        def clean_asn_json(json_data: dict) -> tuple[dict, bool]:
            """
            Detect if the JSON is ASN-related based on key ASN fields.
            If yes, remove `line_items` and return is_asn=True.

            Returns:
                (cleaned_json, is_asn_flag)
            """
            try:
                asn_fields = ["tracking_number", "shipment_date", "carrier_name", "mode_of_transport"]
                is_asn = any(
                    json_data.get(field) not in [None, "", "null", "N/A"]
                    for field in asn_fields
                )

                if is_asn:
                    print("ASN detected. Removing 'line_items'.")
                    json_data.pop("line_items", None)
                else:
                    print("Detected Order Confirmation (not ASN).")

                return json_data, is_asn
            except Exception as e:
                print(f"Error cleaning ASN JSON: {e}")
                return {}, False
 

        def clear_attachment_folder(folder_path="attachments"):
            """Deletes all files and subdirectories inside the given folder."""
            try:
                if os.path.exists(folder_path):
                    shutil.rmtree(folder_path)  # Remove the entire directory
                    os.makedirs(folder_path)  # Recreate the folder to keep the structure intact
                    print(f"Attachment folder '{folder_path}' cleared successfully.")
                else:
                    print(f"Attachment folder '{folder_path}' does not exist.")
            except Exception as e:
                print(f"Error clearing attachment folder: {e}")
    
        def store_po_in_cosmos(collection, formatted_output, extracted_status, is_asn=False, received_datetime=None):
            """
            Store PO or ASN or BOL data in MongoDB.
            For PO: Appends new line items if PO exists and metadata is unchanged.
            For ASN: Appends to 'asn_detail' array.
            For BOL: Appends BOL entries under existing PO in details/missing collection.
            """
            try:
                if all(isinstance(v, dict) for v in formatted_output.values()):
                    for po_number, po_data in formatted_output.items():
                        store_po_in_cosmos(collection, po_data, extracted_status, is_asn=is_asn, received_datetime=received_datetime)
                    return

                po_number = formatted_output.get("po_number")
                print("PO number 1", po_number)
                if not po_number:
                    print("PO Number not found. Cannot store data.")
                    return

                # BOL handling block
                if formatted_output.get("label") == "Bill of Lading":
                    print(f"BOL detected for PO {po_number}")

                    bol_items = []
                    for item in formatted_output.get("line_items", []):
                    #     bol_entry = {
                    #     "po_number": po_number,
                    #     "material_code": item.get("material_code", "N/A"),
                    #     "received_datetime": received_datetime,
                    #     "bol_status": False
                    # }

                    # if bol_entry["material_code"] == "N/A":
                    #     bol_entry["reason"] = "material code not found"

                    # bol_items.append(bol_entry)    
                        bol_items.append({
                            "po_number": po_number,
                            "material_code": item.get("material_code", "N/A"),
                            "received_datetime": received_datetime,
                            "bol_status": False,
                            "missing_field": True if item.get("material_code") == "N/A" else False
                        })
                    
                    # if bol_items["material_code"] == "N/A":
                    #     bol_items["reason"] = "material code not found"   

                    # Check both collections
                    existing_doc = collection.find_one({"_id": po_number}) or missing_collection.find_one({"_id": po_number})

                    if existing_doc:
                        print(f"Existing PO {po_number} found. Appending BOL entries...")

                        existing_bol = existing_doc.get("BOL", [])
                        existing_bol.extend(bol_items)

                        # Deduplicate
                        # unique_bol = list({(b['po_number'], b['material_code']): b for b in existing_bol}.values())

                        # update_fields = {
                        #     "BOL": existing_bol
                        # }

                        # target_collection = collection if collection.find_one({"_id": po_number}) else missing_collection
                        
            
                        target_collection = collection if collection.find_one({"_id": po_number}) else missing_collection
                        # target_collection.update_one({"_id": po_number}, {"$set": existing_bol})
                        for bol in bol_items:
                            target_collection.update_one(
                                {"_id": po_number},
                                {"$push": {"BOL": bol}}
                            )
                        
                        print(f"BOL data updated for PO {po_number}")
                    else:
                        new_doc = {
                            "_id": po_number,
                            "BOL": bol_items
                        }
                        
                        collection.insert_one(new_doc)
                        print(f"Inserted new PO {po_number} with BOL")

                    return  # Skip regular logic for BOL

                # ASN Logic
                formatted_output["status"] = extracted_status
                existing_doc = collection.find_one({"_id": po_number})

                if is_asn:
                    print(f"Detected ASN for PO {po_number}.")
                    formatted_output.pop("line_items", None)
                    asn_payload = {k: v for k, v in formatted_output.items() if k not in ["_id", "status"]}

                    if existing_doc:
                        asn_section = existing_doc.get("asn_detail", [])
                        if not isinstance(asn_section, list):
                            print("Found invalid asn_detail type. Resetting to list.")
                            asn_section = [asn_section] if isinstance(asn_section, dict) else []

                        asn_section.append(asn_payload)
                        existing_doc["asn_detail"] = asn_section
                        collection.replace_one({"_id": po_number}, existing_doc)

                    else:
                        new_doc = {
                            "_id": po_number,
                            "po_number": po_number,
                            "asn_detail": [asn_payload],
                            "status": extracted_status
                        }
                        collection.insert_one(new_doc)
                        print(f"Created new PO {po_number} with ASN detail.")
                    return

                # Order Confirmation logic
                for item in formatted_output.get("line_items", []):
                    item.pop("status", None)

                if existing_doc:
                    print(f"PO {po_number} already exists. Checking for differences...")

                    stored_copy = {k: v for k, v in existing_doc.items() if k not in ["_id", "line_items", "asn_detail"]}
                    current_copy = {k: v for k, v in formatted_output.items() if k != "line_items"}

                    if stored_copy != current_copy:
                        print(f"PO {po_number} metadata differs. Skipping update.")
                        return

                    existing_items = existing_doc.get("line_items", [])
                    new_items = formatted_output.get("line_items", [])
                    combined_items = existing_items + new_items

                    deduped_items = [dict(t) for t in {tuple(sorted(d.items())) for d in combined_items}]

                    updated_doc = {
                        "received_datetime": received_datetime,
                        **existing_doc,
                        **current_copy,
                        "line_items": deduped_items,
                        "status": extracted_status
                    }

                    collection.replace_one({"_id": po_number}, updated_doc)
                    print(f"Line items merged for PO {po_number}.")

                else:
                    collection.insert_one({**formatted_output, "_id": po_number})
                    print(f"PO {po_number} inserted with Order Confirmation fields.")

            except Exception as e:
                print(f"Error storing PO in CosmosDB: {e}")

        
        def create_subscription(access_token):
            """Create a webhook subscription for new email notifications."""
            try:
                expiration_datetime = (datetime.datetime.utcnow() + datetime.timedelta(hours=24)).isoformat() + "Z"
                subscription_data = {
                    "changeType": "created",
                    "notificationUrl": WEBHOOK,
                    "resource": f"users/{EMAIL_ID}/mailFolders('Inbox')/messages",
                    "expirationDateTime": expiration_datetime,
                    "clientState": "K9823jdh"
                }

                headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
                response = requests.post("https://graph.microsoft.com/v1.0/subscriptions", headers=headers, json=subscription_data)

                if response.status_code == 201:
                    with open(SUBSCRIPTION_FILE, "w") as f:
                        json.dump(response.json(), f)
                    print("Subscription created successfully.")
                else:
                    print("Failed to create subscription:", response.json())
            except Exception as e:
                print(f"Error creating subscription: {e}")
        
   
        
     
        def renew_subscription():
            """Automatically renew subscription every 24 hours."""
            try:
                access_token = get_access_token()
                if not access_token:
                    print("Could not renew subscription due to missing token.")
                    return

                if os.path.exists(SUBSCRIPTION_FILE):
                    with open(SUBSCRIPTION_FILE, "r") as f:
                        subscription = json.load(f)
                    subscription_id = subscription.get("id")

                    if subscription_id:
                        expiration_datetime = (datetime.datetime.utcnow() + datetime.timedelta(hours=24)).isoformat() + "Z"
                        renewal_data = {"expirationDateTime": expiration_datetime}
                        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
                        response = requests.patch(f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}", headers=headers, json=renewal_data)

                        if response.status_code == 200:
                            with open(SUBSCRIPTION_FILE, "w") as f:
                                json.dump(response.json(), f)
                            print("Subscription renewed successfully.")
                        else:
                            print("Failed to renew subscription, creating a new one.")
                            create_subscription(access_token)
                    else:
                        create_subscription(access_token)
                else:
                    create_subscription(access_token)
            except Exception as e:
                print(f"Error renewing subscription: {e}")
        

        
        def clear_seen_emails():
            try:
                global seen_emails
                seen_emails.clear()
            # print("Cleared seen_emails cache for reprocessing.")
            except Exception as e:
                print(f"Error clearing seen emails: {e}")
    
        def start_email_monitoring():
            print("start_email_monitoring called")
            try:
                global monitoring_started
                if monitoring_started:
                    print("Email monitor already running. Skipping duplicate launch.")
                    return
                monitoring_started = True
                print("Email monitoring background threads launched.")

                # Start threads (daemon)
                threading.Thread(target=get_recent_emails, args=(access_token,), daemon=True).start()
                threading.Thread(target=worker, daemon=True).start()
                threading.Thread(target=process_attachments, daemon=True).start()

                schedule.every(5).seconds.do(lambda: get_recent_emails(get_access_token()))
                schedule.every(24).hours.do(renew_subscription)
                schedule.every(30).seconds.do(clear_seen_emails)

                def run_scheduler():
                    while True:
                        schedule.run_pending()
                        time.sleep(1)

                threading.Thread(target=run_scheduler, daemon=True).start()

                print("Email monitoring background threads launched.")
                return  # Exit immediately so Azure Function doesn't hang

            except Exception as e:
                print(f"Startup Error: {e}")
        # def start_email_monitoring():
        #     try:
            
        #         # Start the email processing thread
        #         email_processing_thread = threading.Thread(target=get_recent_emails, args=(access_token,), daemon=True)

        #         email_processing_thread.start()

        #         processing_thread = threading.Thread(target=process_attachments, daemon=True)
        #         processing_thread.start()


        #         t1=threading.Thread(target=worker, daemon=True)
        #         t1.start()


        #         # Schedule Tasks
        #         schedule.every(5).seconds.do(lambda: get_recent_emails(get_access_token()))

        #         schedule.every(24).hours.do(renew_subscription)
        #         schedule.every(30).seconds.do(clear_seen_emails)
            
            
        #         def run_scheduler():
        #             while True:
                        
        #                 schedule.run_pending()
        #                 time.sleep(1)

        #         t = threading.Thread(target=run_scheduler, daemon=True)
        #         t.start()
        
                
            
        #         try:
        #             print("Email monitoring service is running...")
        #             while True:
        #                 time.sleep(10)
        #         except KeyboardInterrupt:
        #             print("Stopping email monitoring service...")
        #             stop_event.set()
        #             q.join()
        #             t1.join()
        #     except Exception as e:
        #         import traceback
        #         print("[Startup Error]")
        #         print(f"Error: {e}\nTraceback:\n{traceback.format_exc()}")
except Exception as e:
        import traceback
        print("[Startup Error]")
        print(f"Error: {e}\nTraceback:\n{traceback.format_exc()}")
    