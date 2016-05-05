import logging
import os
import re

def list_py_file_paths(directory):
    file_paths = []
    if os.path.isfile(directory):
        return [directory]
    elif os.path.isdir(directory):
        patterns = []
        for root, dirs, files in os.walk(directory, followlinks=True):
            ignore_file = [f for f in files if f == '.airflowignore']
            if ignore_file:
                f = open(os.path.join(root, ignore_file[0]), 'r')
                patterns += [p for p in f.read().split('\n') if p]
                f.close()
            for f in files:
                try:
                    file_path = os.path.join(root, f)
                    if not os.path.isfile(file_path):
                        continue
                    mod_name, file_ext = os.path.splitext(
                        os.path.split(file_path)[-1])
                    if file_ext != '.py':
                        continue
                    if not any(
                            [re.findall(p, file_path) for p in patterns]):
                        file_paths.append(file_path)
                except Exception as e:
                    logging.exception("Error while examining {}".format(f))
    return file_paths