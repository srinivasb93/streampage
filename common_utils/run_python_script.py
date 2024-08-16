import subprocess


def invoke_python_script(script_path, arguments=[]):
    try:
        # Invoke the Python script using subprocess
        process = subprocess.Popen(['python', script_path] + arguments,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        output, error = process.communicate()

        if error:
            output_text = f"Error: {error.decode()}"
        else:
            output_text = output.decode()
    except Exception as e:
        output_text = f"Error: {str(e)}"

    return output_text


if __name__ == '__main__':
    invoke_python_script('C:/Users/sba400/MyProject/data_analysis_app/pages/etfdata.py')
