#!/bin/bash
set -e

cd ..
git clone --single-branch --branch dev git@github.com:tulibraries/ansible-playbook-combine-docker.git # clone deployment playbook
cd ansible-playbook-combine-docker
sudo pip install pipenv # install playbook requirements
pipenv install # install playbook requirements
pipenv run ansible-galaxy install -r requirements.yml # install playbook role requirements
cp .circleci/.vault ~/.vault # setup vault password retrieval from travis envvar
chmod +x ~/.vault  # setup vault password retrieval from travis envvar

# deploy to qa using ansible-playbook
pipenv run ansible-playbook -i inventory/dev/hosts playbook.yml --vault-password-file=~/.vault -e 'ansible_ssh_port=9229' --private-key=~/.ssh/.conan_the_deployer
