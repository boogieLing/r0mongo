import configparser
import os.path
import re
import bg_helper as bh
import input_helper as ih
from os import getenv, makedirs
from functools import partial
from glob import glob
from shutil import copyfile


APP_ENV = getenv('APP_ENV', 'dev')
separator_rx = re.compile(r'.*[,;\|].*')


def get_default_settings_file(module_name, exception=True):
    """Return path to the default settings.ini file for a module

    - exception: if True, raise an exception if settings.ini not found
    """
    this_dir = os.path.abspath(os.path.dirname(__file__))
    install_dir = os.path.dirname(this_dir)
    found = glob(
        '{}/**/{}/settings.ini'.format(install_dir, module_name),
        recursive=True
    )
    if found:
        return found[0]

    # Possibly a `python3 setup.py develop` situation where a .egg-link file
    # exists in the site-packages directory instead of the module directory
    package_name = module_name.replace('_', '-')
    egg_link_file = os.path.join(install_dir, package_name + '.egg-link')
    if os.path.isfile(egg_link_file):
        with open(egg_link_file, 'r') as fp:
            text = fp.readline()
            linked_path = text.strip()
        default_settings = os.path.join(linked_path, module_name, 'settings.ini')

        if os.path.isfile(default_settings):
            return default_settings

    if exception:
        raise Exception('No default settings.ini found in {} for module {}.'.format(
            repr(install_dir), module_name
        ))


def get_settings_file(module_name, copy_default_if_missing=True, exception=True):
    """Return path to the existing settings.ini file for a module

    - copy_default_if_missing: if True copy the default settings.ini
    - exception: if True, raise an exception if settings file is not found

    Check ~/.config/<pkg>, /etc/<pkg>, /tmp/<pkg> dirs for settings.ini and
    return the first one found

    If copying the default settings.ini file, it will copy to the first of those
    directories that is writeable
    """
    package_name = module_name.replace('_', '-')
    home_config_dir = os.path.expanduser('~/.config/{}'.format(package_name))
    etc_config_dir = '/etc/{}'.format(package_name)
    tmp_config_dir = '/tmp/{}'.format(package_name)
    for dirpath in [home_config_dir, etc_config_dir, tmp_config_dir]:
        settings_file = os.path.join(dirpath, 'settings.ini')
        if os.path.isfile(settings_file):
            return settings_file

    if not copy_default_if_missing:
        if exception:
            raise Exception('Could not find settings.ini in {}'.format(
                ', '.join([home_config_dir, etc_config_dir, tmp_config_dir])
            ))

    if copy_default_if_missing:
        default_settings = get_default_settings_file(module_name, exception=exception)
        if not default_settings:
            return

        for dirpath in [home_config_dir, etc_config_dir, tmp_config_dir]:
            try:
                makedirs(dirpath)
            except FileExistsError:
                pass
            except (PermissionError, OSError):
                continue
            settings_file = os.path.join(dirpath, 'settings.ini')
            try:
                copyfile(default_settings, settings_file)
            except (PermissionError, OSError):
                continue
            else:
                print('copied {} -> {}'.format(repr(default_settings), repr(settings_file)))
                return settings_file


def sync_settings_file(module_name):
    """Use vimdiff to compare default settings file with settings file in use

    Return None if the files already have the same content
    """
    settings_file = get_settings_file(module_name)
    default_settings_file = get_default_settings_file(module_name)
    cmd = 'diff {} {}'.format(repr(settings_file), repr(default_settings_file))
    output = bh.run_output(cmd)
    if output:
        cmd = 'vimdiff {} {}'.format(repr(settings_file), repr(default_settings_file))
        bh.run(cmd)


def _get_config_object(module_name):
    settings_file = get_settings_file(module_name)
    config = configparser.RawConfigParser()
    config.read(settings_file)
    return config


def _get_setting(name, default='', section=None, config_object=None):
    """Get a setting from settings.ini for a particular section (or env var)

    If an environment variable of the same name (or ALL CAPS) exists, return it.
    If item is not found in the section, look for it in the 'default' section.
    If item is not found in the default section of settings.ini, return the
    default value

    The value is converted to a bool, None, int, float if it should be.
    If the value contains any of (, ; |), then the value returned will
    be a list of items converted to (bool, None, int, float, or str).
    """
    val = getenv(name, getenv(name.upper()))
    if not val:
        try:
            val = config_object[section][name]
        except KeyError:
            try:
                val = config_object['default'][name]
            except KeyError:
                return default
            else:
                val = ih.from_string(val)
        else:
            val = ih.from_string(val)
    else:
        val = ih.from_string(val)

    if type(val) == str:
        val = val.replace('\\n', '\n').replace('\\t', '\t')
        if (',' in val or ';' in val or '|' in val):
            val = ih.string_to_converted_list(val)
    return val


def settings_getter(module_name, section=APP_ENV):
    """Return a 'get_setting' func to get a setting from settings.ini for a section"""
    config_object = _get_config_object(module_name)
    return partial(_get_setting, section=section, config_object=config_object)


def get_all_settings(module_name):
    """Return a dict containing all settings from settings.ini"""
    config_object = _get_config_object(module_name)
    sections = set(config_object.sections())
    base = {}
    results = {}
    names = set()
    if 'default' in sections:
        base = dict(config_object['default'])
        sections.discard('default')
        names.update(list(base.keys()))
    for section in sections:
        results[section] = base.copy()
        results[section].update(dict(config_object[section]))
        names.update(list(results[section].keys()))
    env = {name: getenv(name, getenv(name.upper())) for name in names}
    for name, value in env.items():
        for section in sections:
            if name in results[section]:
                if value is not None:
                    results[section][name] = value
                if separator_rx.match(results[section][name]):
                    results[section][name] = ih.string_to_converted_list(
                        results[section][name]
                    )
                else:
                    results[section][name] = ih.from_string(results[section][name])
    return results
