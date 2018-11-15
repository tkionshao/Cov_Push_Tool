# This is a compelet branch for DTAG R3P3
## INSTALL git 2.0 on Centos
sudo yum remove git<br>
sudo yum install epel-release<br>
sudo yum install https://centos6.iuscommunity.org/ius-release.rpm<br>
sudo yum install git2u<br>

git --version

## INSTALL ZIP
sudo yum install zip

## Running script
main.sh
  - backup a all in one sp in sp_release.
  - backup separate single sp in sp_history

git_lab_push.sh
  - commit and push in the one script.
