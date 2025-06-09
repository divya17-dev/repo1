import logging
import json
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Received a new email notification or validation request.")
    
    try:
        # Check for Microsoft Graph validation request
        validation_token = req.params.get('validationToken')
        if validation_token:
            logging.info(f"Received validation token: {validation_token}")
            return func.HttpResponse(validation_token, status_code=200)
        
        # Read and parse the request body
        request_body = req.get_body().decode("utf-8")
        logging.info(f"Raw request body: {request_body}")
        
        data = json.loads(request_body)
        logging.info("Parsed JSON data successfully.")

        # Validate expected structure
        if not isinstance(data, dict) or 'value' not in data:
            logging.error("Invalid payload format: Missing 'value' key.")
            return func.HttpResponse("Invalid payload format", status_code=400)
        
        if not isinstance(data['value'], list) or not data['value']:
            logging.error("Invalid payload: 'value' must be a non-empty list.")
            return func.HttpResponse("Invalid payload structure", status_code=400)
        
        # Extract email data
        notification = data['value'][0]
        email_data = notification.get('resourceData')
        
        if not email_data:
            logging.error("Missing 'resourceData' in notification.")
            return func.HttpResponse("No email data found", status_code=400)
        
        logging.info(f"Extracted email data: {json.dumps(email_data, indent=2)}")
        return func.HttpResponse("Notification processed successfully", status_code=200)
    
    except json.JSONDecodeError as e:
        logging.error(f"JSON parsing error: {e}")
        return func.HttpResponse("Invalid JSON format", status_code=400)
    
    except Exception as e:
        logging.exception("Unexpected error occurred")
        return func.HttpResponse("Internal server error", status_code=500)


# notification url: https://notificationsystem12.azurewebsites.net/api/HttpTrigger