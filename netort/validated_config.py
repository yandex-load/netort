import yaml
import imp
import pkg_resources
import logging

from cerberus import Validator


logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


def load_yaml_schema(path):
    with open(path, 'r') as f:
        return yaml.load(f)


def load_py_schema(path):
    schema_module = imp.load_source('schema', path)
    return schema_module.SCHEMA


def load_schema(directory):
    try:
        return load_yaml_schema(directory)
    except IOError:
        try:
            return load_py_schema(directory)
        except ImportError:
            raise IOError('Neither .yaml nor .py schema found in %s' % directory)


class ValidatedConfig(object):
    def __init__(
            self,
            config,
            dynamic_options,
            package_schema_path,
            package_schema_file='config/schema.yaml',
            with_dynamic_options=True,
            core_section='core'
    ):
        self.BASE_SCHEMA = load_yaml_schema(
            pkg_resources.resource_filename(
                package_schema_path, package_schema_file
            )
        )
        self.DYNAMIC_OPTIONS = dynamic_options
        self._validated = None
        self.META_LOCATION = core_section
        try:
            config[self.META_LOCATION]
        except (NameError, KeyError):
            config[self.META_LOCATION] = {}
        self.__raw_config_dict = config
        self.with_dynamic_options = with_dynamic_options

    def get_option(self, section, option, default=None):
        try:
            self.validated[section][option]
        except KeyError:
            if default:
                return default
            else:
                raise KeyError()
        return self.validated[section][option]

    def get_enabled_sections(self):
        return [
            section_name for section_name, section_config in self.__raw_config_dict.iteritems()
            if section_config.get('enabled', False)
        ]

    def has_option(self, section, option):
        return self.validated

    @property
    def validated(self):
        if not self._validated:
            self._validated = self.__validate()
        return self._validated

    def save(self, filename):
        with open(filename, 'w') as f:
            yaml.dump(self.validated, f)

    def __validate(self):
        core_validated = self.__validate_core()
        errors = {}
        if len(errors) > 0:
            raise ValidationError(dict(errors))
        return core_validated

    def __validate_core(self):
        v = Validator(self.BASE_SCHEMA)
        result = v.validate(self.__raw_config_dict, self.BASE_SCHEMA)
        if not result:
            raise ValidationError(v.errors)
        normalized = v.normalized(self.__raw_config_dict)
        return self.__set_core_dynamic_options(normalized) if self.with_dynamic_options else normalized

    def __set_core_dynamic_options(self, config):
        for option, setter in self.DYNAMIC_OPTIONS.items():
            config[self.META_LOCATION][option] = setter()
        return config

