.PHONY: all

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${ORACLE_HOME}
export LD_LIBRARY_PATH

ANSIBLE_INSTALLED = $(shell ansible --help > /dev/null 2>&1 ; echo $$?)
VIRTUALENV_INSTALLED=$(shell virtualenv --help > /dev/null 2>&1 ; echo $$?)

# Determine the OS type
UNAME_S=$(shell uname -s)

# Check if we're on a RedHat distro
IS_REDHAT=$(shell [ -f /etc/redhat-release ]; echo $$?)

define execute_ansible
	cd ansible; ansible-playbook install.yml -i site  --extra-vars "role_name=$1"; cd ..
endef

all: clean-pyc clean-build install
ansible-all: clean-pyc clean-build ansible-install

clean: clean-pyc clean-build

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} \;
	find . -name '*.pyo' -exec rm -f {} \;
	find . -name '*~' -exec rm -f  {} \;

clean-build:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info

install: install-base install-oracle-client install-postgres-client install-mssql-client
ansible-install: ansible-prereq ansible-install-base ansible-install-oracle-client ansible-install-postgres-client ansible-install-mssql-client

ansible-prereq:
# If virtualenv is not installed and current OS is Redhat
ifneq ($(VIRTUALENV_INSTALLED)$(IS_REDHAT),00)
	sudo yum install -y python-virtualenv
# If virtualenv is not installed for any other OS
else ifneq ($(VIRTUALENV_INSTALLED),0)
	@echo "Please install virtualenv"
else
	@echo "Requirement already satisfied: virtualenv"
endif

# If ansible is not installed and current OS is Redhat
ifneq ($(ANSIBLE_INSTALLED)$(IS_REDHAT),00)
	curl -O https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
	sudo rpm -i epel-release-latest-7.noarch.rpm
	sudo yum install -y ansible
	rm epel-release-latest-7.noarch.rpm
# If ansible is not installed for any other OS
else ifneq ($(ANSIBLE_INSTALLED),0)
	@echo "Please install ansible"
else
	@echo "Requirement already satisfied: ansible"
endif

install-base:
	pip install --upgrade setuptools
	python setup.py install
ansible-install-base: ansible-prereq
	$(call execute_ansible,base)

install-oracle-client: install-base
	pip install 'cx_Oracle >= 5.3'
ansible-install-oracle-client: ansible-install-base
	$(call execute_ansible,oracle-client)

install-postgres-client: install-base
	pip install 'psycopg2 >= 2.7.1'
ansible-install-postgres-client: ansible-install-base
	$(call execute_ansible,postgres-client)


PYMSSQL_INSTALLED_DEFAULT=$(shell python -c "import pymssql" > /dev/null 2>&1 ; echo $$?)

install-mssql-client: install-base
# If pymssql is not installed and OS is MacOS
ifeq ($(PYMSSQL_INSTALLED_DEFAULT)$(UNAME_S),1Darwin)
	pip install git+https://github.com/pymssql/pymssql.git
# If pymssql is not installed and OS is not MacOS
else ifneq ($(PYMSSQL_INSTALLED_DEFAULT),0)
	pip install pymssql
else
	@echo "Requirement already satisfied: pymssql"
endif

PYMSSQL_INSTALLED_VENV=$(shell $$HOME/venvs/dpenv/bin/python -c "import pymssql" > /dev/null 2>&1 ; echo $$?)

ansible-install-mssql-client: ansible-install-base
ifneq ($(PYMSSQL_INSTALLED_VENV),0)
	$(call execute_ansible,mssql-client)
else
	@echo "Requirement already satisfied: pymssql"
endif


test:
	python setup.py test
