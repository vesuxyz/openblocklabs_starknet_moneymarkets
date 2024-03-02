# Money Markets Logic Request

For OpenBlock Labs (OBL) to compute scores for participating Starknet Money Market protocols, we are asking protocols to 
provide logic and code to calculate Value of Non-Recursive Tokens Supplied and Supplier Revenue from Non-Recursive Interest, 
as described in the 
[Starknet Money Markets Data and Calcs Guidelines](https://docs.google.com/document/d/1shpNKHGJbCdTGLUgVCbCpHp6dPbnn6TvGO5sjRc_Yxk/edit?usp=sharing).

This repository provides a framework for sharing this logic with OBL. If you have any questions or comments about this 
process, please contact us in our Telegram channel.

## 1. Getting started

1. Create a fork of this repository and clone it to your development environment.
2. Set up your fork:
   1. Create a `.env` file in the root of the directory, following the format of `example.env`. Populate the fields with 
   your database credentials. These will be safely stored on your local machine. 
   2. Run `./setup.sh` which will setup your virtual environment and install dependencies. **If there is a dependency 
you need in your code, please reach out in Telegram and we will add it ASAP.**
3. Create your protocol directory. Duplicate `protocol_template`, and rename the duplicate with your protocol's name.

## 2. Write your logic

1. Begin writing logic within your directory following the processes and schemas discussed in the 
[Guidelines](https://docs.google.com/document/d/1shpNKHGJbCdTGLUgVCbCpHp6dPbnn6TvGO5sjRc_Yxk/edit?usp=sharing). 
2. write your SQL in `query.sql` and your python function in 
`function.py`. Please demonstrate your calculation in `test.py`.
3. An example SQL query and python function are provided in the `protocol_example` directory.

## 3. How to share your code

1. Make sure your code is working as expected using `test.py`.
2. Commit and push your changes on GitHub.
2. Submit a PR to add your protocol directory to the repository.

## 4. Pull Request Structure

When submitting a pull request, please ensure your contribution meets the following criteria:

- **New Directory**: Each contribution should be within its own new directory. The directory name should be your 
protocol name or some other unique identifier. See `protocol_example` as a reference of contents.
- **Required Files**: Your directory must include at least the following three files:
  - `query.sql`: Contains SQL queries to our snowflake database, creating a table needed for calculations.
  - `function.py`: A Python file with at least one function that computes the specified fields.
  - `test.py`: Contains example demonstration of your code and ensures its functionality.

