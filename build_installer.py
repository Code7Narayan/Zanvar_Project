import os
import shutil
import subprocess

# Define project details
project_name = "Zanvar App"
source_file = "main_app.py"   # Updated entry point
output_dir = "dist"
icon_file = "assets/icon.ico"  # Better to use .ico for Windows executables
version = "1.1.0"

# Step 1: Create executable with PyInstaller
pyinstaller_command = [
    "pyinstaller",
    "--onefile",
    f"--name={project_name}",
    f"--icon={icon_file}",
    "--add-data=assets;assets",  # keep ; for Windows
    "--hidden-import=pyodbc",
    "--hidden-import=tkinter",
    "--hidden-import=PIL",
    source_file
]
subprocess.run(pyinstaller_command, check=True)

# Step 2: Prepare Inno Setup script
inno_script = f"""
[Setup]
AppName={project_name}
AppVersion={version}
DefaultDirName={{pf}}\\{project_name}
DefaultGroupName={project_name}
OutputDir={output_dir}
OutputBaseFilename={project_name}_Setup
Compression=lzma
SolidCompression=yes
ArchitecturesInstallIn64BitMode=x64

[Files]
Source: "{os.path.join(output_dir, project_name + '.exe')}"; DestDir: "{{app}}"
Source: "assets\\*"; DestDir: "{{app}}\\assets"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{{group}}\\{project_name}"; Filename: "{{app}}\\{project_name}.exe"
Name: "{{userdesktop}}\\{project_name}"; Filename: "{{app}}\\{project_name}.exe"

[Run]
Filename: "{{app}}\\{project_name}.exe"; Description: "{{cm:LaunchProgram,{project_name}}}"; Flags: nowait postinstall skipifsilent
"""

with open("installer.iss", "w") as f:
    f.write(inno_script)

# Step 3: Compile Inno Setup script
inno_command = ["ISCC", "installer.iss"]
subprocess.run(inno_command, check=True)

# Step 4: Clean up temporary files
shutil.rmtree("build", ignore_errors=True)
os.remove("installer.iss")

print(f"Installer created at {os.path.join(output_dir, project_name + '_Setup.exe')}")
