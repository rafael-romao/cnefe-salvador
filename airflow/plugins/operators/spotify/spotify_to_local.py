import pandas as pd
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from httpx import Client
from httpx._exceptions import HTTPError
from typing import Any, Dict, List
from re import search
from pathlib import Path

log = logging.getLogger(__name__)


class SpotifyToRaw(BaseOperator):
    @apply_defaults
    def __init__(self,
                 account_url,
                 base_url,
                 client_id,
                 client_secret,
                 offset,
                 limit,
                 show_name,
                 data_dir_path,
                 raw_path,
                 *args, **kwargs):
        super(SpotifyToRaw, self).__init__(*args, **kwargs)
        
        self.account_url = account_url
        self.base_url = base_url
        self.client = self._client(self.base_url)
        self.client_id = client_id
        self.client_secret = client_secret
        self.offset = offset
        self.limit = limit
        self.data_dir_path = data_dir_path
        self.raw_path = raw_path
        self.show_name = show_name
        self.token = self._token_request()
        self.headers = {'Authorization': f'Bearer {self.token}'}
            
    

    def execute(self, context):
        shows_json = self._get_show_data()
        if shows_json:
            parsed_show_data = self._parse_show_data(shows_json)
        else:
            raise Exception(f'No data: {shows_json}')
        dh_id = self._get_dh_id(parsed_show_data)
        dh_episode_json = self._get_dh_episodes(dh_id)
        if dh_episode_json:
            parsed_dh_episode = self._parse_dh_episodes(dh_episode_json)
        else:
            raise Exception(f'No data: {dh_episode_json}')

        gb_episode_data_list = self._get_gb_episode_in_dh_list(parsed_dh_episode)
        gb_episode_df = self._list_to_df(list_name='GB Episode',json_list=gb_episode_data_list)
        dh_episode_df = self._list_to_df(list_name='DH Episode', json_list=parsed_dh_episode)
        shows_df = self._list_to_df(list_name='Shows', json_list=parsed_show_data)
        self._load_to_local(gb_episode_df, "dh_gb_episodes.csv")
        self._load_to_local(dh_episode_df, "dh_episodes.csv")
        self._load_to_local(shows_df, "shows.csv")
        return None

    def _client(self, base_url):
        self.log.info("Getting the client")
        client = Client(base_url=base_url)
        return client

    def _token_request(self):              
        self.log.info("Getting the token")
        headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
        }
        params = {
        'grant_type': 'client_credentials', 
        'client_id': self.client_id, 
        'client_secret': self.client_secret
        }
        with Client(base_url=self.account_url) as token_client:
            response = token_client.post(
                url='api/token',  
                headers=headers,
                params=params
            )
        if response.status_code != 200:
            raise Exception(
                f'Error while getting the token. Please, check your credentials at .env'
            )
        else:
            token = response.json().get('access_token')
        return token
    
    def _get_show_data(self):
        self.log.info("Getting the shows data")           
        url_query = 'search?'
        url_query+=f'q={self.show_name}'
        url_query+=f'&type=show'
        url_query+=f'&market=BR'
        url_query+=f'&limit={self.limit}'
        url_query+=f'&offset={self.offset}'       
        
        try:            
            response = self.client.get(
                url=url_query,  
                headers=self.headers
            )
            response.raise_for_status()
            show_data = response.json()
        except HTTPError as http_error:
            self.log.error(f'HTTP Error: {http_error.args[0]}')
        return show_data

    def _get_dh_episodes(self, dh_id):
        offset=0
        counter=0
        more_runs=1
        dh_episode_data_list = []        
        
        while((offset <= 1950) & (counter <= more_runs)):       
            episodes_endpoint = f'shows/{dh_id}/episodes?'
            show_query = episodes_endpoint
            show_query += f'&offset={offset}'            
            show_query += f'&limit={self.limit}'
            show_query += f'&market=BR'   
        
            dh_response = self.client.get(
                url=show_query,  
                headers=self.headers,
            )
            dh_episodes_json = dh_response.json()
            offset = offset + 50
            counter += 1
            if next(iter(dh_episodes_json)) != 'error':
                dh_episode_data_list.append(dh_episodes_json)                
                more_runs = (dh_episodes_json.get('total') // 50)
            else:
                offset = 1000000
                raise Exception(
                    f'{dh_response}'
                )
        return dh_episode_data_list
    
    def _get_gb_episode_in_dh_list(self, dh_episode_list):        
        gb_episodes_list = []
        self.log.info(f'Length of {self.show_name} episode list: {len(dh_episode_list)}')
        for episode in dh_episode_list:
            has_gb = search(r'\bGrupo Botic[aá]rio', episode.get('episode_description'))
            if has_gb:
                gb_episodes_list.append(episode)
            else:
                continue
        return gb_episodes_list
    
    def _get_dh_id(self, show_data_list:List[Dict[str, Any]]) -> str:
        self.log.info(f"Parsing the {self.show_name} id")
        dh_id = "".join(str(show.get('show_id')) for show in show_data_list if show.get('show_name') == self.show_name)
        return dh_id 


    def _parse_show_data(self, data):
        self.log.info(f"Parsing the show data")
        
        show_data_list = []
        for item in data.get('shows').get('items'):
            data = {
                'show_id': item.get('id'),
                'show_name': item.get('name'),
                'show_description': item.get('description'),
                'show_total_episodes': item.get('total_episodes')        
            }  
            show_data_list.append(data)        
        return show_data_list
    
    def _parse_dh_episodes(self, data_list):
        self.log.info(f"Parsing the {self.show_name} episodes data")        
        dh_episode_data_list = []
        for dh_episode_json in data_list:
            for item in dh_episode_json.get('items'):
                data = {
                    'episode_id': item.get('id'),
                    'episode_description': item.get('description'),
                    'episode_duration': item.get('duration_ms'),
                    'episode_release_date': item.get('release_date')
                }  
                dh_episode_data_list.append(data)
        return dh_episode_data_list

    
    def _load_to_local(self, df,filename):        
        path = f"{self.data_dir_path}{self.raw_path}"
        Path(path).mkdir(parents=True, exist_ok=True)
        final_path = f"{path}/{filename}"
        self.log.info(f"Loading the data to {final_path}") 
        df.to_csv(final_path, index=False)
    
    def _list_to_df(self, list_name, json_list:Dict[str, Any]) -> pd.DataFrame:
        self.log.info(f"Turning {list_name} list into a DF")
        df = pd.DataFrame.from_records(json_list)
        return df    
    
    