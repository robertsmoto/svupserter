The SvUpserter application looks first for /etc/svupserter/conf.json, and if it is not available, then will try and open the file ./conf-default.json.

We recommend that you copy the default file to /etc/svupserter/conf.json and assign your configuration variables there.

The .json config variables are described here:

{
    "DB_HOST": "", # database host
    "DB_NAME": "", # database name
    "DB_USER": "", # database user name
    "DB_PASS": "", # database password
    "DB_PORT": "", # databas port
    "API_AID": "", # svApi account id (uuid)
    "API_AUT": "", # svApi auth (uuid)
    "API_PRE": "", # svApi prefix (uuid)
    "API_CTT": ""  # svapi content-type
}
