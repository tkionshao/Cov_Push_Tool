#! /bin/bash
source ./instance.cnf
read -p "commit description: " comment

git add .
git commit -m "${comment}"
git push origin ${BRANCHNAME} 
