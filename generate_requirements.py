import os
import re
import subprocess

def find_imports(directory):
    imports = set()
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                with open(os.path.join(root, file), 'r') as f:
                    content = f.read()
                    matches = re.findall(r'^\s*import\s+(\S+)', content, re.MULTILINE)
                    matches += re.findall(r'^\s*from\s+(\S+)', content, re.MULTILINE)
                    imports.update(matches)
    return imports

def write_temp_script(imports):
    with open("temp_script.py", "w") as f:
        for imp in imports:
            f.write(f"import {imp}\n")

def generate_requirements():
    subprocess.run(["pipreqs", ".", "--force"], check=True)

def convert_requirements_to_pipfile():
    subprocess.run(["pipenv", "install", "-r", "requirements.txt"], check=True)

def cleanup():
    os.remove("temp_script.py")

if __name__ == "__main__":
    directory = "./"  # Change this to the target directory
    imports = find_imports(directory)
    write_temp_script(imports)
    generate_requirements()
    convert_requirements_to_pipfile()
    cleanup()
