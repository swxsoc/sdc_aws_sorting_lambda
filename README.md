# SWSOC File Sorting Lambda Function

### **Base Image Used For Development Container:** https://github.com/HERMES-SOC/docker-lambda-base 

### **Description**:
This repository is to define the code to be used for the SWSOC file sorting Lambda function. This function will be deployed as a zip file to Lambda, with the production lambda being the latest release and the latest code on the master being used for development and testing. The production lambda will move files into the appropriate buckets while the development lambda will only move files with the prefix `dev_`. 

### **Testing Locally**:
1. Open up the repo in locally in VSCode. It will detect the `.devcontainer` and ask if you'd like to open it in a container. Agree and a container development environment will start up with all of the required tools. 

2. Test running by setting by changing the event and running the `lambda_function_test_script.py` file.

# Information on working with a CDK Project

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands for CDK

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation