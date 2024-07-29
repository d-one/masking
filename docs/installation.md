# Project Installation Guide

This guide will help you set up and install the project dependencies using the Makefile. The Makefile will ensure that `pip` and `poetry` are installed, and then use `poetry` to install the project dependencies.

## Prerequisites

Before you begin, ensure you have the following installed on your machine:

- **Python 3.x**: The project requires Python 3.11.0. You can download it from [python.org](https://www.python.org/downloads/).

## Steps

1. **Clone the repository**:
    ```sh
    git clone https://github.com/d-one/masking.git
    cd masking
    ```

2. **Run the Makefile**:
    The Makefile contains commands to check for `pip` and `poetry`, and install them if they are not already installed. It then installs the project dependencies using `poetry`.

    To run the Makefile, use the following command:
    ```sh
    make install
    ```

3. **Verify the installation**:
    After running the `make` command, verify that the virtual environment has been created and the dependencies are installed.

    By running
    ```bash
    ls -a .
    ```

    You should see a `.venv` directory in the project root.

## Additional Information

- Package management is done through [Poetry](./poetry.md).

## Troubleshooting

If you encounter any issues during the installation, please check the following:

- Ensure that you have Python 3.11 installed and it is added to your system's PATH.
- Ensure that you have the necessary permissions to install packages and create directories.

For further assistance, feel free to open an issue in the repository or contact the project maintainers.

---

By following these steps, you should be able to set up your development environment and start working on the project. Happy coding!
