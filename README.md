# Data Pipeline

## Synopsis

Data Pipeline is a Python application for replicating data from source to
target databases; supporting the full workflow of data replication from the
initial synchronisation of data, to the subsequent near real-time
Change Data Capture.

Further documentation (high-level design, component design, etc.) can be found
in the "docs" directory.

## Motivation

This project is borne out of the need for real-time analytics of data, with
minimal impact on the database housing the original data.

## Installation

There are two options available for installation:
- Automated
- Manual

The Automated option takes advantage of the idempotent operations that Ansible
offers, along with potential to deploy Data Pipeline to multiple servers. There
is no prerequisite to install Ansible as the Makefile will do this for you.
Furthermore, a Python virtualenv (venvs/dpenv) will be created automatically
with all Python dependencies installed within that directory.

Note that, at the time of writing, the Automated installation has only been
tested against RedHat 7.4.

The Manual installation option requires manual installation of package
dependencies followed by Python package dependencies.

### Prerequisites

- Python 2.7
- Sudo/Root Access
- (optional) Oracle Instant Client downloaded (see next section)

### Downloading Oracle Instant Client

A prerequisite to installing the oracle client modules for this project
is the availability of the Instant Client files provided by Oracle.

#### RHEL/Centos

Download the following oracle instantclient files located at
http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html:

```
oracle-instantclient12.2-basic-12.2.0.1.0-1.x86_64.rpm
oracle-instantclient12.2-devel-12.2.0.1.0-1.x86_64.rpm
```

into the /tmp/oracle directory of the server where the installation will be
executed from.

#### MacOS X

Download and install the following oracle instantclient files located at
http://www.oracle.com/technetwork/topics/intel-macsoft-096467.html:

```
instantclient-basic-macos.x64-12.1.0.2.0.zip
instantclient-sdk-macos.x64-12.1.0.2.0.zip
```

### Automated Installation

While in the project root directory, run the following

```
make ansible-install
source $HOME/venvs/dpenv/bin/activate
source $HOME/.bash_profile
```

### Manual Installation

The manual installation option allows one to have a custom setup; for instance,
if one wishes to run Python from the root-owned Python virtual environment, 
use a different virtual environment from the one pre-configured in this
project.

#### RHEL/Centos

The following are the manual steps involved to install the system dependencies
for a RedHat/Centos distribution. There are plans to automate this procedure
via ansible.

```
# Install the EPEL
curl -O https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
sudo rpm -i epel-release-latest-7.noarch.rpm

# Update installed yum packages
sudo yum -y update

sudo yum -y install python-pip
sudo yum groupinstall -y 'Development Tools'

sudo rpm -ivh /tmp/oracle/oracle-instantclient12.2-basic-12.2.0.1.0-1.x86_64.rpm
sudo rpm -ivh /tmp/oracle/oracle-instantclient12.2-devel-12.2.0.1.0-1.x86_64.rpm

export ORACLE_HOME=/usr/lib/oracle/12.2/client64/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME/lib

# Optional: Update ~/.bashrc or ~/.bash_profile to export the above environment
# variables for each session

sudo yum install -y java-1.8.0-openjdk-1.8.0.121
sudo yum install -y python-devel
sudo yum install -y python-wheel
sudo yum install -y libaio
sudo yum install -y freetds
sudo yum install -y freetds-devel
sudo yum install -y librdkafka-devel
sudo yum install -y yaml-cpp-devel
sudo yum install -y python-cffi
sudo yum install -y libffi-devel
sudo yum install -y openssl-devel
```

### Mac OS

Firstly, you'll need to install the oracle instant client.
The zip files can be found on:

http://www.oracle.com/technetwork/topics/intel-macsoft-096467.html

... and instructions on how to install it can be found here:

http://joelvasallo.com/?p=485

Then install the following packages via brew:

```
brew install freetds
brew install librdkafka
brew install postgresql
brew install libyaml-dev
```

#### Python Dependencies

The following command will perform a full installation of all dependencies
(including client packages for all supported source and target databases)
via pip:

```
make
```

#### Custom Installation

You may also perform a custom install by first installing the base
dependencies:

```
make install-base
```

Followed by whatever database clients you require. Examples include:

```
make install-oracle-client
make install-postgres-client
make install-mssql-client
```

## Tests

To run tests, execute the following command:

```
make test
```

## Setting up database login credentials

There are three database endpoints that Data Pipeline connects to:

- Source: The source database to extract data from
- Target: The target database to apply data to
- Audit: The database storing data of the extract and apply processes for
         monitoring and auditing purposes.

These credentials are defined by a connection string over command line via
the following arguments:

```
--sourcedbuser <username/password@host:port/dbname>
--targetdbuser <username/password@host:port/dbname>
--audituser <username/password@host:port/dbname>
```

Clearly, it is not good practice to publish one's password over command line as
it will be visible on the process list (and potentially any calling in shell
scripts).

As such, one should omit the password component of the connection string like so:

```
--sourcedbuser bob@myhost:1234/abc
```

... which will cause the Data Pipeline components (extractor, applier,
initsync) to request, at run-time, the passwords (via stdin) on each of
the required database connections. These credentials will then be stored on
the operating system's keystore. For example, on Keychain on MacOS, and
~/.local/share/python_keyring/keyring_pass.cfg on RedHat.

Another option is to preemptively set these passwords via the keyring tool, which
is installed as part of the "pip install keyring" step.

For example, to add credentials for a database called "abc" on host "myhost"
with port 1234 and username "bob", run the command:

```
keyring set "myhost:1234/abc" "bob"
Password for 'bob' in 'myhost:1234/abc':
```

Credentials can also be queried and removed using the same tool:

```
keyring get "myhost:1234/abc" "bob"
keyring del "myhost:1234/abc" "bob"
```

## Running

The following are templates for the most common commandline options
used for the Data Pipeline components, namely: InitSync, Extractor and Applier.

Note that all config file parameters will be overridden by its respective
command-line parameter.

### InitSync

Please refer to conf/sample_initsync_config.yaml for an example config file.

```
python -m data_pipeline.initsync_pipe --config /path/to/initsync_config.yaml
```

### Extractor

Please refer to conf/sample_extractor_config.yaml for an example config file.
```
python -m data_pipeline.extract --config /path/to/extractor_config.yaml
```

### Applier

Please refer to conf/sample_applier_config.yaml for an example config file.
```
python -m data_pipeline.apply --config /path/to/applier_config.yaml
```
