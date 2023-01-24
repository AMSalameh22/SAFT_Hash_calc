from six import iteritems
import hashlib
import base64
import os
import OpenSSL
import datetime
import mysql.connector

PEM = '''-----BEGIN PRIVATE KEY-----
MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAMz7yE1bbpqWfLzt
QJudqwAeV1mzQj8DQj4tO45Cf2VVrHne1qra5qpRjG7lhPqkwrHpgjd3Rm32DtMo
5Fc7oerN61SWg92HBCTBHOpvDNt7df1Dqiadklt8TJcXmpt3HstTfJL8q/GHaSo3
fmnQZFNgLUpwrAcUzTlukDO/zsULAgMBAAECgYEAr9rR3Iu0N6sjGHHyG48IRPHC
vpGrI6QIEI99qG4kHSuC7IrgX3OssDGF6R2/F8iBv2A09qC9K1kNHtrWCoB+RwmA
eKmYSkuOXwssEPqlgKI1CdXNRkT3l+po5v2HIfAMSm3gSk4Jk6T8S/sz2HtLL6S5
hTQXNj90iCIBdBAxtAECQQDprZX0wLQpVZ5h8M2GFSNJ56IBsG1AYqBAsQXfF7OQ
AGrOYk96q6Sapr3pcZQkkIvsfXVh301TFCtuIDs98mVRAkEA4JB+eg3RDW7N8Phn
6p0dIW4Z7vcei2JN8lC+EWYPIxVeXGcVsR5OwK9CPEZxgR6e38G1YNjFmeWa8IMa
YC1dmwJBAN4xfGEZyzVygANI4WQZPVDMu7M3euq53P8mVBrpgofunaBYwpI2G3Ii
Enqi0OZju2jHcpH5rqDMkO9KHoqsrcECQESGEvHvkWEF9LWZNyxyMNdon2U55wol
Px+336ezet77wEV19zqKwwEs+Ysm+6+oxlYE5/Hbc8CYPYUynDPI5A0CQQCAMKzz
HMvjyJvlXryczXZ2iH9w+y9VsZrgHPjIcCh8xWsMdVV5myLmWLlgco5v6b+Ao6jI
+QAmKp21LOURK8x/
-----END PRIVATE KEY-----
'''

def via_pyopenssl(message, digest="sha1"):
		key = OpenSSL.crypto.load_privatekey(OpenSSL.crypto.FILETYPE_PEM, PEM)
		return OpenSSL.crypto.sign(key, message, digest)

def updatehash(creation,name,grand_total,previoushash):
    InvoiceDate= str(creation).split(' ')[0]
    systemEntryDate =str(creation).replace(' ','T').split('.')[0]
    InvoiceNo = str(name)
    GrossTotal =  "{:.2f}".format(float(str(grand_total).replace('-','')))
    str2encrypt = InvoiceDate+';'+systemEntryDate+';'+InvoiceNo+';'+GrossTotal+';'+previoushash
    encrypted = via_pyopenssl(str2encrypt)
    encoded = base64.b64encode(encrypted)
    return encoded.decode()

#naming_series=['PP ALV.YYYY./.#','PP IT.YYYY./.#','PP MAC.YYYY./.#','PP MAI.YYYY./.#','PP VIA.YYYY./.#']
naming_series=['GR IT.YYYY./.#']
try:
    # Connect to the database
    cnx = mysql.connector.connect(user='root', password='root',
                                  host='127.0.0.1', database='_cdae0cba0e19d8d6')

    # Create a cursor object to execute queries
    cursor = cnx.cursor(buffered=True)
    cursor2 = cnx.cursor(buffered=True)
    for name_series in naming_series:
        # Select all rows from a table
        print("Name Series : " + name_series)
        #query = 'SELECT creation,name,grand_total FROM tabQuotation where naming_series=%s order by creation'
        query = 'SELECT creation,name,grand_total FROM `tabDelivery Note` where naming_series=%s order by creation'
        cursor.execute(query,(name_series,))
        previoushash=''
        # Iterate over each row
        for row in cursor:
            try:
                # Update a specific column for the current row
                print("Doc Name : " + row[1])
                print("previous Hash : " + previoushash)
                previoushash=updatehash(row[0],row[1],row[2],previoushash)
                #update_query = "UPDATE tabQuotation SET saft_hash=%s WHERE name=%s"
                update_query = "UPDATE `tabDelivery Note` SET saft_hash=%s WHERE name=%s"
                cursor2.execute(update_query, (previoushash,row[1],))
                print("New Hash : " + previoushash)
                cnx.commit()
            except mysql.connector.Error as e:
                print(f'Error: {e}')
                cnx.rollback()
        # Commit the changes

except mysql.connector.Error as e:
    print(f'Error: {e}')

finally:
    # Close the cursor and connection
    cursor.close()
    cnx.close()
