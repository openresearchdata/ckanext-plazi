import logging
import json
import os
import requests
import tempfile
import zipfile
import csv
import shutil

from ckan.model import Session
from ckan.logic import get_action
from ckan import model

from ckanext.harvest.harvesters.base import HarvesterBase
from ckan.lib.munge import munge_tag
from ckan.lib.munge import munge_title_to_name
from ckanext.harvest.model import HarvestObject

log = logging.getLogger(__name__)


class PlaziHarvester(HarvesterBase):
    '''
    Plazi Harvester
    '''

    HARVEST_USER = 'harvest'

    def info(self):
        '''
        Return information about this harvester.
        '''
        return {
            'name': 'plazi_harvester',
            'title': 'Plazi harvester',
            'description': 'Harvester for Plazi data sources'
        }

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}

        if 'user' not in self.config:
            self.config['user'] = self.HARVEST_USER

        log.debug('Using config: %r' % self.config)

    def gather_stage(self, harvest_job):
        '''
        The gather stage will recieve a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its source and job.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''
        log.debug("in gather stage: %s" % harvest_job.source.url)
        try:
            harvest_obj_ids = []
            self._set_config(harvest_job.source.config)
            plazi_url = harvest_job.source.url.rstrip('/')

            r = requests.get(plazi_url)
            data = r.json()

            count = 0
            for entry in data:
                # TODO remove this limitation
                if count == 10:
                    break
                log.debug(entry)
                harvest_obj = HarvestObject(
                    guid=entry['UUID'],
                    job=harvest_job,
                    content=json.dumps(entry)
                )
                harvest_obj.save()
                harvest_obj_ids.append(harvest_obj.id)
                count = count + 1
        except:
            log.exception(
                'Gather stage failed %s' %
                harvest_job.source.url
            )
            self._save_gather_error(
                'Could not gather anything from %s!' %
                harvest_job.source.url, harvest_job
            )
            return None
        return harvest_obj_ids

    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        log.debug("in fetch stage: %s" % harvest_object.guid)
        self._set_config(harvest_object.job.source.config)
        return True

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g
              create a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package must be added to the HarvestObject.
              Additionally, the HarvestObject must be flagged as current.
            - creating the HarvestObject - Package relation (if necessary)
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        log.debug("in import stage: %s" % harvest_object.guid)
        self._set_config(harvest_object.job.source.config)

        if not harvest_object:
            log.error('No harvest object received')
            self._save_object_error('No harvest object received')
            return False

        try:
            context = {
                'model': model,
                'session': Session,
                'user': self.config['user']
            }

            package_dict = {}
            content = json.loads(harvest_object.content)
            log.debug(content)

            package_dict['id'] = harvest_object.guid
            package_dict['name'] = munge_title_to_name(content['title'])

            mapping = self._get_mapping()

            for ckan_field, plazi_field in mapping.iteritems():
                try:
                    package_dict[ckan_field] = content[plazi_field]
                except (IndexError, KeyError):
                    continue

            package_dict['maintainer'] = 'Guido Sautter'
            package_dict['maintainer_email'] = 'sautter@ipd.uka.de'

            # add owner_org
            source_dataset = get_action('package_show')(
              context,
              {'id': harvest_object.source.id}
            )
            owner_org = source_dataset.get('owner_org')
            package_dict['owner_org'] = owner_org

            # add resources
            treatments = self._read_taxa_file(content['darwinCoreArchive'])
            package_dict['resources'] = self._extract_resources(treatments)

            '''
            TODO implement all fields
            # extract tags from 'type' and 'subject' field
            # everything else is added as extra field
            tags, extras = self._extract_tags_and_extras(content)
            package_dict['tags'] = tags
            package_dict['extras'] = extras

            # groups aka projects
            groups = []

            # create group based on set
            if content['set_spec']:
                log.debug('set_spec: %s' % content['set_spec'])
                groups.extend(
                    self._find_or_create_groups(
                        content['set_spec'],
                        context
                    )
                )

            # add groups from content
            groups.extend(
                self._extract_groups(content, context)
            )

            package_dict['groups'] = groups
            '''

            # allow sub-classes to add additional fields
            package_dict = self._extract_additional_fields(
                content,
                package_dict
            )

            log.debug('Create/update package using dict: %s' % package_dict)
            self._create_or_update_package(
                package_dict,
                harvest_object
            )

            Session.commit()

            log.debug("Finished record")
        except:
            log.exception('Something went wrong!')
            self._save_object_error(
                'Exception in import stage',
                harvest_object
            )
            return False
        return True

    def _get_mapping(self):
        return {
            'title': 'title',
            'url': 'link',
            'author': 'author',
            'license_id': 'rights'
        }

    def _extract_tags_and_extras(self, content):
        extras = []
        tags = []
        for key, value in content.iteritems():
            if key in self._get_mapping().values():
                continue
            if key in ['type', 'subject']:
                if type(value) is list:
                    tags.extend(value)
                else:
                    tags.extend(value.split(';'))
                continue
            if value and type(value) is list:
                value = value[0]
            if not value:
                value = None
            extras.append((key, value))

        tags = [munge_tag(tag[:100]) for tag in tags]

        return (tags, extras)

    def _extract_resources(self, treatments):
        resources = []

        for treatment in treatments:
            resource_type = 'HTML'
            resources.append({
                'name': treatment['scientificName'],
                'resource_type': resource_type,
                'format': resource_type,
                'url': treatment['references']
            })
        return resources

    def _read_taxa_file(self, url):
        temp_dir = tempfile.mkdtemp()
        dwca_file_name = url.split('/')[-1]
        dwca_file_path = os.path.join(temp_dir, dwca_file_name)
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            r.raise_for_status()
        with open(dwca_file_path, 'wb') as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        taxa_file_path = self._unzip(dwca_file_path, temp_dir)

        treatments = self._read_treatments(taxa_file_path)

        # delete temp directory and content
        shutil.rmtree(temp_dir)

        return treatments

    def _unzip(self, f, temp_dir):
        z = zipfile.ZipFile(f)
        taxa_file_path = z.extract('taxa.txt', temp_dir)
        z.close()
        return taxa_file_path

    def _read_treatments(self, taxa_file_path):
        with open(taxa_file_path,'rb') as taxa_file:
            reader = csv.DictReader(taxa_file, delimiter='\t')
            treatments = [row for row in reader]

        return treatments

    def _extract_additional_fields(self, content, package_dict):
        # This method is the ideal place for sub-classes to
        # change whatever they want in the package_dict
        return package_dict

    def _find_or_create_groups(self, groups, context):
        log.debug('Group names: %s' % groups)
        group_ids = []
        for group_name in groups:
            data_dict = {
                'id': group_name,
                'name': munge_title_to_name(group_name),
                'title': group_name
            }
            try:
                group = get_action('group_show')(context, data_dict)
                log.info('found the group ' + group['id'])
            except:
                group = get_action('group_create')(context, data_dict)
                log.info('created the group ' + group['id'])
            group_ids.append(group['id'])

        log.debug('Group ids: %s' % group_ids)
        return group_ids