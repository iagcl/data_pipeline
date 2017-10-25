# Contributing to Data Pipelines

When contributing to this repository, please first discuss the change you wish
to make via issue, email, or any other method with the owners of this
repository before making a change.

Please note we have a code of conduct, please follow it in all your
interactions with the project.

## Pull Request Process

0. (Optional) Install the pre-commit hook using the instructions outlined
   in the "Test and Code Coverage Pre-commit Hooks" section below.
1. Implement a test for the proposed change if possible.
2. Ensure all tests pass by running `make test` from the project root.
3. Update the README.md and other relevant files with details of changes.
   This includes new arguments introduced when running the application,
   additional dependencies and useful file locations.
4. Increase the version numbers in the setup.py file to the
   new version that this Pull Request would represent. The versioning scheme
   we use is [SemVer](http://semver.org/).
5. You may merge the Pull Request in once you have the sign-off of two other
   developers, or if you do not have permission to do that, you may request
   the second reviewer to merge it for you.

### Test and Code Coverage Pre-commit Hooks

It is strongly advised that execution of unit tests, code coverage and
pep8 coding standards checks be included within the your pre-commit
hooks to help improve code quality of commits.

To do this, create a file in `data_pipeline/.git/hooks/pre-commit` with the
following contents:

```
#!/bin/sh
#
# To enable this hook, rename this file to "pre-commit".

export ORACLE_HOME=/Users/albertteoh/Applications/oracle/instantclient_12_1
export LD_LIBRARY_PATH=$ORACLE_HOME

rootdir=$(git rev-parse --show-toplevel)
echo "Moving to $rootdir"
cd $rootdir

# Remove compiled code
find . -name "*pyc" -delete

# Execute style checks. Ignore:
pep8 --exclude=*/ui/*,tests*/data*,*mssql* data_pipeline tests

# Execute unit tests
echo "Executing unit tests and coverage report..."
py.test --cov=data_pipeline tests/
```

## Code of Conduct

### Our Pledge

In the interest of fostering an open and welcoming environment, we as
contributors and maintainers pledge to making participation in our project and
our community a harassment-free experience for everyone, regardless of age,
body size, disability, ethnicity, gender identity and expression, level of
experience, nationality, personal appearance, race, religion, or sexual
identity and orientation.

### Our Standards

Examples of behavior that contributes to creating a positive environment
include:

* Using welcoming and inclusive language
* Being respectful of differing viewpoints and experiences
* Gracefully accepting constructive criticism
* Focusing on what is best for the community
* Showing empathy towards other community members

Examples of unacceptable behavior by participants include:

* The use of sexualized language or imagery and unwelcome sexual attention or
advances
* Trolling, insulting/derogatory comments, and personal or political attacks
* Public or private harassment
* Publishing others' private information, such as a physical or electronic
  address, without explicit permission
* Other conduct which could reasonably be considered inappropriate in a
  professional setting

### Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable
behavior and are expected to take appropriate and fair corrective action in
response to any instances of unacceptable behavior.

Project maintainers have the right and responsibility to remove, edit, or
reject comments, commits, code, wiki edits, issues, and other contributions
that are not aligned to this Code of Conduct, or to ban temporarily or
permanently any contributor for other behaviors that they deem inappropriate,
threatening, offensive, or harmful.

### Scope

This Code of Conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community. Examples of
representing a project or community include using an official project e-mail
address, posting via an official social media account, or acting as an
appointed representative at an online or offline event. Representation of a
project may be further defined and clarified by project maintainers.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be
reported by contacting the project team . All
complaints will be reviewed and investigated and will result in a response that
is deemed necessary and appropriate to the circumstances. The project team is
obligated to maintain confidentiality with regard to the reporter of an
incident.
Further details of specific enforcement policies may be posted separately.

Project maintainers who do not follow or enforce the Code of Conduct in good
faith may face temporary or permanent repercussions as determined by other
members of the project's leadership.

## Attribution

This CONTRIBUTING page is based on an excellent template provided on
[PurpleBooth's GitHub][purplebooth-homepage], available at the
[Good-CONTRIBUTING template][good-contributing-page] page.

The Code of Conduct is adapted from the [Contributor Covenant][homepage],
version 1.4, available at
[http://contributor-covenant.org/version/1/4][version].

[homepage]: http://contributor-covenant.org
[version]: http://contributor-covenant.org/version/1/4/
[purplebooth-homepage]: https://gist.github.com/PurpleBooth
[good-contributing-page]: https://gist.github.com/PurpleBooth/b24679402957c63ec426
