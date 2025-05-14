# Money Markets Logic Request

For OpenBlock Labs (OBL) to compute scores for participating Starknet Money Market protocols, we are asking protocols to 
provide code for calculations as described in the
[Starknet Money Markets New Data Guidelines](https://docs.google.com/document/d/12EbTqFgCppEtUX9TBe9hTxGtvFFWRS62bqjXzDpkXVE/edit?usp=sharing).

This repository provides a framework for sharing your code with OBL. If you have any questions or comments about this 
process, please contact us in our Telegram channel.

## 1. Getting started

1. Create a fork of this repository and clone it to your development environment. If you already have a fork, please update it.
2. Set up your fork:
   1. Create a `.env` file in the root of the directory, following the format of `example.env`. Populate the fields with 
   your database credentials. These will be safely stored on your local machine. 
   2. Run `./setup.sh` which will setup your virtual environment and install dependencies. **If there is a dependency 
you need in your code, please reach out in Telegram and we will add it ASAP.**
3. Create your protocol directory. Duplicate `protocol_template`, and rename the duplicate with your protocol's name.

## 2. Write your code

1. Write your code within your directory following the processes and schemas discussed in the 
[Guidelines](https://docs.google.com/document/d/1uQddC1KB4lDLGWuTuGtWAxR11N1Nromz90rmgmpks1U/edit).
2. If your code has any dependencies beyond those available in the `requirements.txt` file, please add them to the requirements and include them in your PR.

## 3. How to share your code

1. Make sure your code is working as expected.
2. Commit and push your changes to your fork.
2. Submit a PR to add your protocol directory to the repository.

## 4. Pull Request Structure

When submitting a pull request, please ensure your contribution meets the following criteria:

- **New Directory**: Each contribution should be within its own new directory. The directory name should be your 
protocol name. See `protocol_example` as a reference of contents.
- **Required Files**: Your directory should include at least one python script:
  - `function.py`: A Python file with at least one function that computes the specified fields.

