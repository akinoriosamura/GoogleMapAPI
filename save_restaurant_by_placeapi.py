import googlemaps
import time
import numpy as np
import pandas as pd
import sys


class GoogleMapAPI(object):
    def __init__(self, lat, lng, radius):
        self.key = '' # 上記で作成したAPIキーを入れる
        self.client = googlemaps.Client(self.key) #インスタンス生成
        # geocode_result = client.geocode('東京都渋谷駅') # 位置情報を検索
        # loc = geocode_result[0]['geometry']['location'] # 軽度・緯度の情報のみ取り出す
        # loc = {'lat': lat, 'lng': lng}
        self.params = {
                'location': (lat, lng),
                'radius': radius,
                'type': 'restaurant',
                'language': 'ja'
            }
        self.places_columns = ['geo_loc_lat', 'geo_loc_lng', 'geo_vp_ne_lat', 'geo_vp_ne_lng', 'geo_vp_sw_lat', 'geo_vp_sw_lng', \
'icon', 'id', 'name', 'photos_heights', 'photos_html_attributions', 'photos_photo_reference', 'photos_width', \
'place_id', 'plus_code_compound_code', 'plus_code_global_code', 'price_level', 'rating', 'reference', 'scope', \
'types', 'user_ratings_total', 'vicinity']

    def get_nearby(self):
        place_result = self.client.places_nearby(**self.params)
        results = place_result['results']
        # import pdb;pdb.set_trace()
        if 'next_page_token' in place_result.keys():
            page_token = place_result["next_page_token"]
        else:
            page_token = ""

        return results, page_token

    def get_mat_result(self, results):
        mat_result = []
        for result in results:
            l_result = []
            if 'geometry' in result.keys():
                geo_loc_lat = result['geometry']['location']['lat']
                geo_loc_lng = result['geometry']['location']['lng']
                geo_vp_ne_lat = result['geometry']['viewport']['northeast']['lat']
                geo_vp_ne_lng = result['geometry']['viewport']['northeast']['lng']
                geo_vp_sw_lat = result['geometry']['viewport']['southwest']['lat']
                geo_vp_sw_lng = result['geometry']['viewport']['southwest']['lng']
                l_result.extend([geo_loc_lat, geo_loc_lng, geo_vp_ne_lat, geo_vp_ne_lng, geo_vp_sw_lat, geo_vp_sw_lng])
            else:
                l_result.extend([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
            if 'icon' in result.keys():
                l_result.append(result['icon'])
            else:
                l_result.append("")
            if 'id' in result.keys():
                l_result.append(result['id'])
            else:
                l_result.append("")
            if 'name' in result.keys():
                l_result.append(result['name'])
            else:
                l_result.append("")
            if 'photos' in result.keys():
                p_value = result['photos'][0]
                photos_heights = p_value['height']
                photos_html_attributions = p_value['html_attributions'][0]
                photos_photo_reference = p_value['photo_reference']
                photos_width = p_value['width']
                l_result.extend([photos_heights, photos_html_attributions, photos_photo_reference, photos_width])
            else:
                l_result.extend([np.nan, "", "", np.nan])
            if 'place_id' in result.keys():
                l_result.append(result['place_id'])
            else:
                l_result.append("")
            if 'plus_code' in result.keys():
                plus_code_compound_code = result['plus_code']['compound_code']
                plus_code_global_code = result['plus_code']['global_code']
                l_result.extend([plus_code_compound_code, plus_code_global_code])
            else:
                l_result.extend(["", ""])
            if 'price_level' in result.keys():
                l_result.append(result['price_level'])
            else:
                l_result.append(np.nan)
            if 'rating' in result.keys():
                l_result.append(result['rating'])
            else:
                l_result.append(np.nan)
            if 'reference' in result.keys():
                l_result.append(result['reference'])
            else:
                l_result.append("")
            if 'scope' in result.keys():
                l_result.append(result['scope'])
            else:
                l_result.append("")
            if 'types' in result.keys():
                types = ",".join(result['types'])
                l_result.append(types)
            else:
                l_result.append("")
            if 'user_ratings_total' in result.keys():
                l_result.append(result['user_ratings_total'])
            else:
                l_result.append(np.nan)
            if 'vicinity' in result.keys():
                l_result.append(result['vicinity'])
            else:
                l_result.append("")

            # import pdb;pdb.set_trace()
            if len(l_result) != len(self.places_columns):
                import pdb;pdb.set_trace()
            assert len(l_result) == len(self.places_columns), "dont match length l_result with places_columns"
            mat_result.append(l_result)
    
        return mat_result


if __name__ == "__main__":
    args = sys.argv
    assert len(args) == 4, "set save csv name, lat, lon in arg"
    lat = args[1]
    lng = args[2]
    csv_name = args[3]

    googemapapi = GoogleMapAPI(lat, lng, 500)
    results, page_token = googemapapi.get_nearby()
    mat_result = googemapapi.get_mat_result(results)
    print("results mat length: ", len(mat_result))
    while True:     
        if page_token == "":
            break
        time.sleep(10)
        googemapapi.params["page_token"] = page_token
        results, page_token = googemapapi.get_nearby()
        print(page_token)
        mat_result_ = googemapapi.get_mat_result(results)
        mat_result.extend(mat_result_)
        print("results mat length: ", len(mat_result))

    df = pd.DataFrame(mat_result, columns=googemapapi.places_columns)

    #  save the csv
    df.to_csv("./" + csv_name)


