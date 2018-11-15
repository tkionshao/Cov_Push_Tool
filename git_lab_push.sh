#! /bin/bash
source ./instance.cnf
read -p "commit description: " comment

git pull origin ${BRANCHNAME}
git checkout ${BRANCHNAME}
git add .
git commit -m "${comment}"
git push origin ${BRANCHNAME} 


