import os
import re
import sys
import time
import json
import gzip
import io

import requests as req
from requests import Session

from datetime import datetime, timedelta

import errors
from enums import Mode, Error, StatusCode, TimeOut
from httpclient import HttpClient

import sendgrid

class TikTok:

    def __init__(self, httpclient, output, mode, logger, url=None, user=None, room_id=None, use_ffmpeg=None, duration=None,
                 convert=False):
        self.output = output
        self.url = url
        self.user = user
        self.mode = mode
        self.room_id = room_id
        self.use_ffmpeg = use_ffmpeg
        self.duration = duration
        self.convert = convert
        self.logger = logger
        self.last_email_sent_time = None
        self.from_email = 'live-notify@majorhustler.com'
        self.template_id = 'd-4f7c1413ca05439a98c81f42a83d1cb2'
        self.list_id = '3a6b6a56-ad52-4461-b2f1-967f8a9eb475'

        self.sg = sendgrid.SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))

        #self.get_contacts()

        #self.send_dynamic_template({'tiktok': self.user})

        if httpclient is not None:
            self.httpclient: Session = httpclient.req
        else:
            self.httpclient = req

        if self.url is not None:
            self.user, self.room_id = self.get_room_and_user_from_url()

        if self.user is None:
            self.user = self.get_user_from_room_id()
        if self.room_id is None:
            self.room_id = self.get_room_id_from_user()

        self.logger.info(f"USERNAME: {self.user}")
        self.logger.info(f"ROOM_ID:  {self.room_id}")

        is_blacklisted = self.is_country_blacklisted()
        if mode == Mode.AUTOMATIC and is_blacklisted:
            raise ValueError(Error.AUTOMATIC_MODE_ERROR)

        # I create a new httpclient without proxy
        self.httpclient = HttpClient(self.logger, None).req

    def run(self):
        """
        runs the program in the selected mode. 
        
        If the mode is MANUAL, it checks if the user is currently live and if so, starts recording. 
        
        If the mode is AUTOMATIC, it continuously checks if the user is live and if not, waits for the specified timeout before rechecking.
        If the user is live, it starts recording.
        """
        if self.mode == Mode.AUTOMATIC:
            while True:
                client_offline = False
                try:
                    self.room_id = self.get_room_id_from_user()
                except BaseException:
                    client_offline = True
                if not self.is_user_in_live():
                    self.logger.info(f"{'Client' if client_offline else self.user} is offline")
                    self.logger.info(f"waiting {TimeOut.AUTOMATIC_MODE} minutes before recheck\n")
                    time.sleep(TimeOut.AUTOMATIC_MODE * TimeOut.ONE_MINUTE)
                    continue

                current_time = datetime.now()
                if self.last_email_sent_time is None or \
                        current_time - self.last_email_sent_time >= timedelta(hours=1):
                    self.send_dynamic_template({'tiktok': self.user})
                    self.last_email_sent_time = current_time
                else:
                    self.logger.info(f"Skipping email notification. Last email sent: {self.last_email_sent_time}")
                    time.sleep(TimeOut.AUTOMATIC_MODE * TimeOut.ONE_MINUTE)

    def get_contacts(self):
        response = self.sg.client.marketing.contacts.exports.post({'list_ids': [self.list_id], 'file_type': 'json'})
        data = json.loads(response.body)
        self.logger.info(data)
        id = data['id']
     
        status = 'pending'
        while status != 'ready':
            response = self.sg.client.marketing.contacts.exports._(id).get()
            contacts = json.loads(response.body)
            status = contacts['status']
            if status == 'failure':
                self.logger.error(contacts)
                break
            time.sleep(5)
          
        # Download and parse gzipped JSON data from contacts['urls'][0]
        response = req.get(contacts['urls'][0])
        self.logger.info(response.content)
        contacts_data = [json.loads(line)['email'] for line in response.content.decode().split('\n') if line]
        
        self.logger.info(contacts_data)

        return contacts_data


    def send_dynamic_template(self, dynamic_data={}):
    
        # Get all contacts
        response = self.sg.client.marketing.contacts.get()
        data = json.loads(response.body)
        emails = self.get_contacts()
    
        if len(emails) > 0:
            message = sendgrid.Mail(
                from_email=self.from_email,
                to_emails=emails,
                is_multiple=True
            )
            message.template_id = self.template_id
            message.dynamic_template_data = dynamic_data
            try:
                response = self.sg.send(message)
                self.logger.info(f"Email sent to {emails}. Status code: {response.status_code}")
            except Exception as e:
                self.logger.error(e.message)

    def get_live_url(self) -> str:
        """
        I get the cdn (flv or m3u8) of the streaming
        """
        try:
            url = f"https://webcast.tiktok.com/webcast/room/info/?aid=1988&room_id={self.room_id}"
            json = self.httpclient.get(url).json()

            if 'This account is private' in json:
                raise errors.AccountPrivate('Account is private, login required')

            live_url_flv = json['data']['stream_url']['rtmp_pull_url']
            self.logger.info(f"LIVE URL: {live_url_flv}")

            return live_url_flv
        except errors.AccountPrivate as ex:
            raise ex
        except Exception as ex:
            self.logger.error(ex)

    def is_user_in_live(self) -> bool:
        """
        Checking whether the user is live
        """
        try:
            url = f"https://www.tiktok.com/api/live/detail/?aid=1988&roomID={self.room_id}"
            content = self.httpclient.get(url).text

            return '"status":4' not in content
        except ConnectionAbortedError:
            if self.mode == Mode.MANUAL:
                self.logger.error(Error.CONNECTION_CLOSED)
            else:
                self.logger.error(Error.CONNECTION_CLOSED_AUTOMATIC)
                time.sleep(TimeOut.CONNECTION_CLOSED * TimeOut.ONE_MINUTE)
            return False
        except Exception as ex:
            self.logger.error(ex)

    def get_room_and_user_from_url(self):
        """
        Given a url, get user and room_id.
        """
        try:
            response = self.httpclient.get(self.url, allow_redirects=False)
            content = response.text

            if response.status_code == StatusCode.REDIRECT:
                raise errors.Blacklisted('Redirect')

            if response.status_code == StatusCode.MOVED:  # MOBILE URL
                regex = re.findall("com/@(.*?)/live", response.text)
                if len(regex) < 1:
                    raise errors.LiveNotFound(Error.LIVE_NOT_FOUND)
                self.user = regex[0]
                self.room_id = self.get_room_id_from_user()
                return self.user, self.room_id

            self.user = re.findall("com/@(.*?)/live", content)[0]
            self.room_id = re.findall("room_id=(.*?)\"/>", content)[0]
            return self.user, self.room_id

        except (req.HTTPError, errors.Blacklisted):
            raise errors.Blacklisted(Error.BLACKLIST_ERROR)
        except Exception as ex:
            self.logger.error(ex)
            exit(1)

    def get_room_id_from_user(self) -> str:
        """
        Given a username, I get the room_id
        """
        try:
            response = self.httpclient.get(f"https://www.tiktok.com/@{self.user}/live", allow_redirects=False)
            if response.status_code == StatusCode.REDIRECT:
                raise errors.Blacklisted('Redirect')

            content = response.text
            if "room_id" not in content:
                raise ValueError()

            return re.findall("room_id=(.*?)\"/>", content)[0]
        except (req.HTTPError, errors.Blacklisted) as e:
            raise errors.Blacklisted(Error.BLACKLIST_ERROR)
        except ValueError:
            self.logger.error(f"Unable to find room_id. I'll try again in {TimeOut.CONNECTION_CLOSED} minutes")
            time.sleep(TimeOut.CONNECTION_CLOSED * TimeOut.ONE_MINUTE)
            return self.get_room_id_from_user()
        except AttributeError:
            if self.mode != Mode.AUTOMATIC:
                raise errors.UserNotFound(Error.USERNAME_ERROR)
            time.sleep(TimeOut.CONNECTION_CLOSED * TimeOut.ONE_MINUTE)
        except Exception as ex:
            self.logger.error(ex)
            exit(1)

    def get_user_from_room_id(self) -> str:
        """
        Given a room_id, I get the username
        """
        try:
            url = f"https://www.tiktok.com/api/live/detail/?aid=1988&roomID={self.room_id}"
            content = self.httpclient.get(url).text

            if "LiveRoomInfo" not in content:
                raise AttributeError(Error.USERNAME_ERROR)

            return re.search('uniqueId":"(.*?)",', content).group(1)
        except Exception as ex:
            self.logger.error(ex)
            exit(1)

    def is_country_blacklisted(self) -> bool:
        """
        Checks if the user is in a blacklisted country that requires login
        """
        try:
            response = self.httpclient.get(f"https://www.tiktok.com/@{self.user}/live", allow_redirects=False)
            return response.status_code == StatusCode.REDIRECT
        except Exception as ex:
            self.logger.error(ex)
