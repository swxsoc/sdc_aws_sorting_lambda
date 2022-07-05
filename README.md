# SWSOC File Sorting Lambda Function

### **Base Image Used For Development Container:** https://github.com/HERMES-SOC/docker-lambda-base 

### **Description**:
This repository is to define the code to be used for the SWSOC file sorting Lambda function. This function will be deployed as a zip file to Lambda, with the production lambda being the latest release and the latest code on the master being used for development and testing. The production lambda will move files into the appropriate buckets while the development lambda will only move files with the prefix `dev_`. 

### **Testing Locally**:
1. Open up the repo in locally in VSCode. It will detect the `.devcontainer` and ask if you'd like to open it in a container. Agree and a container development environment will start up with all of the required tools. 

2. Test running by setting by changing the event and running the `lambda_function_test_script.py` file.