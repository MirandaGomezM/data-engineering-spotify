import spotipy
from spotipy.oauth2 import SpotifyOAuth

sp_oauth = SpotifyOAuth(client_id = 'tu_cliente_id',
client_secret = 'tu_cliente_secreto',
redirect_uri = 'http://localhost:8888/callback',
scope = 'user-top-read')

# Este código te redirigirá al enlace de autorización
token_info = sp_oauth.get_access_token()
print(token_info)