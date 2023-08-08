import webbrowser
import requests
import msal

APP_ID = "c9eb157c-a854-4438-aca2-0a72b6866c8f"
DICT_ID = "acfde157-8636-4952-b4e3-ed8fd8e274e9"
CLIENT_SECRET = "T4E8Q~7fpSTGKCoTxeg0_ss11LJYOaQ-McwRobAi"
AUTH_URL = "https://login.microsoftonline.com/consumers/"
BASE_URL = "https://graph.microsoft.com/v1.0/"
SCOPES = ["User.Read"]

client_instance = msal.ConfidentialClientApplication(
  client_id = APP_ID,
  client_credential = CLIENT_SECRET,
  authority = AUTH_URL,
)

# auth_request_url = client_instance.get_authorization_request_url(SCOPES)
# print(auth_request_url)
# webbrowser.open(auth_request_url, new=True)

auth_code = "M.C105_BAY.2.b0ba4deb-62a7-bf20-07db-10270c260811"
access_token = client_instance.acquire_token_by_authorization_code(
  code = auth_code,
  scopes = SCOPES
)

access_token_id = access_token["access_token"]
headers = {"Authorization": f"Bearer {access_token_id}"}

# END_POINT_ME = BASE_URL + "me"
# response =  requests.get(END_POINT_ME, headers=headers)
# print(response.json())

END_POINT_ME_DRIVE_RECENT = BASE_URL + "me/drive/recent"
response =  requests.get(END_POINT_ME_DRIVE_RECENT, headers=headers)
print(response.json())
