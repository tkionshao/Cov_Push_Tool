#! /bin/bash
VERSION=$1
source ./instance.cnf
read -p "commit description: " comment

# git pull origin ${BRANCHNAME}
git checkout ${BRANCHNAME}
git add .
# git tag -a ${VERSION} -m '${comment}'
git commit -m "${VERSION} - ${comment}"
git push origin ${BRANCHNAME} 


