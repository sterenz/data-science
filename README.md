# Data Science Project 🧪

## Software to process data stored in different formats and to upload them into two distinct databases to query these databases simultaneously according to predefined operations.

# 💻 Dev info

## ☕️ JAVA enviroment for OS

To run Blazegraph in your local sytem you need JAVA.
To check you version of JAVA:

```bash
java --version
```

In case you don't have JAVA already installed in your Mac OS system.

Install Homebrew for Mac OS official documentation

[Homebrew homepage](https://brew.sh/)

Than run:

```bash
brew install java
```

If you need to have openjdk first in your PATH, run:

zsh:
```zsh
echo 'export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"' >> ~/.zshrc
```
bash:
```bash
echo 'export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"' >> ~/.bashrc
```

## ⛓ Libss

```
requirements.txt
```

Packages, libraries and modules wich this project needs are listed in this file alreday in project.

To get all required libs to run this program, write the cmd below in your terminal opened in the project folder:

```bash
python3 -m pip install -r requirements.txt
```

## 👟 Run

To run this program, write the cmd below in your terminal opened in the project folder, with all libs already installed:

```bash
python3 main.py
```

## 📥 Results

All queries results are produced here:

```bash
./queries-results
```

For each query is produced a file with related name `<query-name>.txt` .
