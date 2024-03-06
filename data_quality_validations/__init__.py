def import_all():
    from os.path import dirname, basename, isfile, join
    
    dir_name = dirname(__file__)
    quality_folder = dir_name.split('/')[-1]
    
    print(dir_name)
    print(quality_folder)
    
    if ('.zip' in dir_name.lower()) or ('.whl' in dir_name.lower()):
        import zipfile
        zip_path = dir_name.replace('/'+quality_folder, '')
        print(zip_path)
        zipobj = zipfile.ZipFile(zip_path)
        paths = [ quality_folder + '.' + i.split('/')[-1].replace('.py','') for i in zipobj.namelist() if quality_folder +'/' in  i and '__init__.py' not in i and '.py' in i]
        print(paths)
    else:
        import glob
        modules = glob.glob(join(dir_name, "*.py"))
        paths = [ quality_folder + '.' + basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py')]

    return paths

paths = import_all()
for import_path in paths:
    print(import_path)
    exec(f'from {import_path} import *')