#######################################
## Nome: Thomaz Antonio Rossito Neto ##
#######################################
#        Objetivo deste script        #
#######################################
# 1- Realizar o Download dos arquivos #
# que estão no FTP para um diretório  #
# local                               #
# 2- Realizar a extração dos arquivos #
# da RAIS que estão no formato 7-Zip  #
# 3- Realizar a compressão dos        #
# arquivos no formato TXT para GZIP   #
# 4 - Realizar o Upload dos arquivos  #
# da RAIS para o bucket S3            # 
#######################################
#         Para que funcione           #
#######################################
# 1- Realizar ajuste no "path_master" #
# informando onde os arquivos da RAIS #
# estão no diretório local            #
# 2- Realizar o ajuste no "bucket_s3" #
# para o nome do bucket que já existe #
# no ambiente de Cloud                #
#######################################


################################### Importe Libs #################################################
import os
import boto3
from pyunpack import Archive
import gzip
import shutil
from ftplib import FTP
import os


################################### Nome Usuario #################################################
user: str   = os.getlogin()


####################################### Paths ####################################################
# path_master = f"C:/Users/{user}/OneDrive/Documentos/file" ## WINDOWS - Ajustar esse path 
path_master = "D:/RAIS2020"

path_7zip   = "/DATA_7ZIP/"   # Ajustar esse path onde os arquivos 7zip estão
path_txt    = "/DATA_TXT/"
path_gzip   = "/DATA_GZIP/"

bucket_s3     = 'tarn-datalake-raw-433046906551'   # Ajustar nome do bucket
bucket_s3_sub = "RAIS-2020" 

extension_7z  = ".7z"
extension_txt = ".txt"
extension_gz  = ".gz"


################################### Download Files FTP ###########################################
# print("")
# print(f"Início download do FTP...")

# ftp = FTP('ftp.mtps.gov.br')
# ftp.login() 
# files = ftp.nlst("pdet/microdados/rais/2020/")
# ftp.cwd("pdet/microdados/rais/2020/")

# ## Multiple Files
# for i in files:
#     print(f"{path_master}{path_7zip}" + i[26:])
#     ftp.retrbinary("RETR " + i[26:], open(f"{path_master}{path_7zip}" + i[26:], 'wb').write)

# ftp.close()

# print(f"Fim download do FTP...")


################################### 7-ZIP -> TXT #################################################
dir_source_7z = f"{path_master}{path_7zip}"
dir_targe_txt = f"{path_master}{path_txt}"

# print("")
# print(f"Iniciando o unzip 7-ZIP -> TXT...")

# for filename in os.listdir(path=dir_source_7z):
#     if filename.endswith(extension_7z):
#         print(filename)
#         Archive(f"{dir_source_7z}/{filename}").extractall(f"{dir_targe_txt}")

# print(f"Fim o unzip 7-ZIP -> TXT...")


################################### TXT -> GZIP ###################################################
dir_source_gzip = f"{path_master}{path_gzip}"

# print("")
# print(f"Iniciando o zip TXT -> GZIP...")

# for namefile in os.listdir(path=dir_targe_txt):
#     if namefile.endswith(extension_txt):
#         print(namefile)
#         with open(f"{dir_targe_txt}/{namefile}", 'rb') as f_in:
#             with gzip.open(f"{dir_source_gzip}/{namefile}" + extension_gz, 'wb') as f_out:
#                 shutil.copyfileobj(f_in, f_out)

# print(f"Fim GZIP -> Upload S3...")
# print("")


################################### GZIP -> Upload S3 #################################################
client = boto3.client("s3")

print("")
print(f"Iniciando GZIP -> Upload S3...")
print("")

for namefilegz in os.listdir(path=dir_source_gzip):
    if namefilegz.endswith(extension_gz):
        print(namefilegz)
        client.upload_file(f"{dir_source_gzip}{namefilegz}", 
                   f"{bucket_s3}", 
                   f"{bucket_s3_sub}/{namefilegz}")

print("")
print(f"Fim GZIP -> Upload S3...")                   

################################### FIM ###################################################