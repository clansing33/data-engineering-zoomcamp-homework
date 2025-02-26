# WSL Spark Setup

# 1. Install Java Development Kit (JDK):

Apache Spark requires Java to run. It's recommended to install OpenJDK 11.

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
```
After installation, verify the Java version:


```bash
java -version
```
You should see output indicating Java 11 is installed.

# 2. Download and Install Apache Spark:

Navigate to a directory where you want to install Spark, then download and extract it:

```bash
# Navigate to your home directory or any preferred directory
cd ~

# Download Spark 3.3.2
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz

# Extract the downloaded file
tar xzfv spark-3.3.2-bin-hadoop3.tgz

# Move the extracted directory to /opt for system-wide use
sudo mv spark-3.3.2-bin-hadoop3 /opt/spark

# Remove the downloaded archive to clean up
rm spark-3.3.2-bin-hadoop3.tgz
```

3. Set Environment Variables:

To easily run Spark commands, add Spark to your system's PATH. Edit your .bashrc file to include the following:

```bash
# Open .bashrc in a text editor
nano ~/.bashrc
```
Add these lines at the end of the file:

```bash
# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Set SPARK_HOME
export SPARK_HOME=/opt/spark

# Add Spark and Java to PATH
export PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin
```
Save and exit the editor (in nano, press CTRL + X, then Y, and Enter). To apply the changes, reload the .bashrc file:

```bash
source ~/.bashrc
```
# 4. Verify the Installation:

Start the Spark shell to ensure everything is set up correctly:

```bash
spark-shell
```
If installed correctly, you'll enter the Spark interactive shell. To exit, type:

```scala
:quit
```
For Python users, you can start the PySpark shell:

```bash
pyspark
```
To exit, type exit() or press CTRL + D.

By following these steps, you should have Apache Spark successfully installed on your Ubuntu setup within WSL on Windows 11.

For a visual walkthrough of the installation process, you might find this video helpful:
https://youtu.be/ei_d4v9c2iA


# Install Jupyter Lab
## 1. Open the VS Code Terminal in WSL
Open VS Code in WSL (as we did earlier).
Open a terminal inside VS Code (Ctrl + ~ or from the menu: Terminal > New Terminal).
Ensure you’re inside your WSL environment by running:
```bash
which python3
```
It should return a path like /usr/bin/python3, confirming that you’re in WSL.
## 2. Install Jupyter Lab
Run the following command inside your VS Code terminal:

```bash
pip install jupyterlab
```

Now, check where the executables (like jupyter) are installed:

```bash
python3 -m site --user-base
```

This should return:

```
/home/your-username/.local
```
If that's the case, Jupyter should be in:

```
/home/your-username/.local/bin
```

## 3. Fix the PATH
If jupyter exists in ~/.local/bin, add it to your PATH:

```bash
export PATH=$HOME/.local/bin:$PATH
```
Make it permanent:

```bash
echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```



# Create a Working Directory
Run this in your WSL terminal:

```bash
mkdir -p ~/workspace/spark-course
cd ~/workspace/spark-course
```

# Download the Parquet File Here
Once inside your spark-course directory, download your Parquet file. For example, if it's available via a URL, use:

```bash
wget <parquet-file-url>
```

# Run Jupyter from pyspark
If you want to avoid setting paths manually, you can launch Jupyter from a PySpark session so it inherits Spark's environment:

```bash
pyspark --driver-memory 2g --executor-memory 2g --conf spark.ui.port=4040
```
Then, in the PySpark shell, type:

```python
import os
os.system("jupyter lab --no-browser --ip=0.0.0.0")
```