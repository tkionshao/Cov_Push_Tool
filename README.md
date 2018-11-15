# INSTALL git 2.0 on Centos
sudo yum remove git
sudo yum install epel-release
sudo yum install https://centos7.iuscommunity.org/ius-release.rpm
sudo yum install git2u

git --version

# INSTALL ZIP
sudo yum install zip

# Running script
main.sh
  - backup a all in one sp in sp_release.
  - backup separate single sp in sp_history

git_lab_push.sh
  - commit and push in the one script.
