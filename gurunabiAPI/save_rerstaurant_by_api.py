# coding: utf-8

import pandas as pd
import requests
import json
import time

class GurunabiAPI(object):
    def __init__(self):
        #レストラン検索APIのURL
        key = ""
        self.result_url = "https://api.gnavi.co.jp/RestSearchAPI/v3/"
        self.prefs_url = "https://api.gnavi.co.jp/master/PrefSearchAPI/v3/"

        #パラメータの設定
        self.result_params={}
        self.result_params["keyid"] = key #取得したアクセスキー
        self.result_params["range"] = 1 # 300m radius
        # self.result_params["pref"] = "PREF13" # 東京
        # self.result_params["offset"] = 1
        self.result_params["hit_per_page"] = 100
        self.result_params["sort"] = 1 # 店舗名順
        # self.result_params["name"] = "花見鮨"
        self.prerfs_params={}
        self.prerfs_params["keyid"] = key #取得したアクセスキー
        self.prerfs_params["lang"] = "ja" # 目黒

        # 42
        self.columns = [
            'id', 'update_date', 'name', 'name_kana', 'latitude', \
            'longitude', 'category', 'url', 'url_mobile', 'coupon_url_pc', 'coupon_url_mobile', \
            'image_url_im1', 'image_url_im2', 'image_url_qr', 'address', 'tel', 'tel_sub', 'opentime', \
            'holiday', 'access_line', 'access_sta', 'access_sta_exit', 'access_walk', 'access_note', 'parking_lots', \
            'pr_short', 'pr_long', 'areacode', 'areaname', 'prefcode', 'prefname', 'areacode_s', 'areaname_s', \
            'category_code_l', 'category_name_l', 'category_code_s', 'category_name_s', \
            'budget', 'party', 'lunch', 'credit_card', 'e_money'
            ]
        # count store num
        self.store_num = 0
        # api call num
        self.api_num = 0

    def get_prefs(self):
        prefs_api = requests.get(self.prefs_url, self.prerfs_params)
        prefs_api = prefs_api.json()

        return prefs_api

    def reshape_json(self, result_jsons):
        reshaped_results = []
        for result in result_jsons:
            reshaped_result = []
            reshaped_result.append(result['id'])
            reshaped_result.append(result['update_date'])
            reshaped_result.append(result['name'])
            reshaped_result.append(result['name_kana'])
            reshaped_result.append(result['latitude'])
            reshaped_result.append(result['longitude'])
            reshaped_result.append(result['category'])
            reshaped_result.append(result['url'])
            reshaped_result.append(result['url_mobile'])
            reshaped_result.append(result['coupon_url']['pc'])
            reshaped_result.append(result['coupon_url']['mobile'])
            reshaped_result.append(result['image_url']['shop_image1'])
            reshaped_result.append(result['image_url']['shop_image2'])
            reshaped_result.append(result['image_url']['qrcode'])
            reshaped_result.append(result['address'])
            reshaped_result.append(result['tel'])
            reshaped_result.append(result['tel_sub'])
            reshaped_result.append(result['opentime'])
            reshaped_result.append(result['holiday'])
            reshaped_result.append(result['access']['line'])
            reshaped_result.append(result['access']['station'])
            reshaped_result.append(result['access']['station_exit'])
            reshaped_result.append(result['access']['walk'])
            reshaped_result.append(result['access']['note'])
            reshaped_result.append(result['parking_lots'])
            reshaped_result.append(result['pr']['pr_short'])
            reshaped_result.append(result['pr']['pr_long'])
            reshaped_result.append(result['code']['areacode'])
            reshaped_result.append(result['code']['areaname'])
            reshaped_result.append(result['code']['prefcode'])
            reshaped_result.append(result['code']['prefname'])
            reshaped_result.append(result['code']['areacode_s'])
            reshaped_result.append(result['code']['areaname_s'])
            reshaped_result.append(",".join(result['code']['category_code_l']))
            reshaped_result.append(",".join(result['code']['category_name_l']))
            reshaped_result.append(",".join(result['code']['category_code_s']))
            reshaped_result.append(",".join(result['code']['category_name_s']))
            reshaped_result.append(result['budget'])
            reshaped_result.append(result['party'])
            reshaped_result.append(result['lunch'])
            reshaped_result.append(result['credit_card'])
            reshaped_result.append(result['e_money'])

            assert len(reshaped_result) == len(self.columns), "dont match result and columns length"
            reshaped_results.append(reshaped_result)

        return reshaped_results

    def get_results(self ,lat, lng):
        results = []
        self.result_params["latitude"] = lat
        self.result_params["longitude"] = lng
        print("lat: ", self.result_params["latitude"])
        print("lng: ", self.result_params["longitude"])
        #リクエスト結果
        self.api_num += 1
        print("api num: ", self.api_num)
        if self.api_num > 7000:
            time.sleep(3600)
        result_api = requests.get(self.result_url, self.result_params)
        result_api = result_api.json()
        # print(result_api)
        if 'error' in result_api.keys():
            assert result_api['error'][0]['code'] != 429, "アクセス上限いっぱいだからapi_num調整してもう一度"
            return results
        self.store_num += len(result_api['rest'])
        reshaped_results = self.reshape_json(result_api['rest'])
        results.extend(reshaped_results)
        # MAX1000のレスポンスなので、100 * 10だけ確認
        for page_i in range(2, 11):
            print("page i : ", page_i)
            gurunabiapi.result_params["offset_page"] = page_i
            self.api_num += 1
            print("api num: ", self.api_num)
            if self.api_num > 7000:
                time.sleep(3600)
            result_api = requests.get(self.result_url, self.result_params)
            result_api = result_api.json()
            if 'error' in result_api.keys():
                assert result_api['error'][0]['code'] != 429, "アクセス上限いっぱいだからapi_num調整してもう一度"
                break
            self.store_num += len(result_api['rest'])
            reshaped_results = self.reshape_json(result_api['rest'])
            results.extend(reshaped_results)

        return results

if __name__ == '__main__':
    gurunabiapi = GurunabiAPI()
    # prefs_api = gurunabiapi.get_prefs()
    # print(prefs_api)
    # 東京周辺の緯度経度範囲：
    # 北南：35.49 - 35.83
    # 東西：139.26 - 139.92
    # 400m = 0.0037(range = 1は半径300mの円内のレストランを取得するので200m四方の正方形をカバーできるため)
    min_lat = 35.49
    max_lat = 35.83
    min_lng = 139.26
    max_lng = 139.92
    det_lat_lng = 0.0037
    # first request
    results = gurunabiapi.get_results(min_lat, min_lng)
    print("results length: ", len(results))
    print("store num: ", gurunabiapi.store_num)
    print("----------------")
    # det_lat_lngの個数を計算
    ns_iter = round((max_lat - min_lat) / det_lat_lng)
    ew_iter = round((max_lng - min_lng) / det_lat_lng)
    lat = min_lat
    lng = min_lng
    for ns_idx in range(ns_iter):
        for ew_idx in range(ew_iter):
            new_lat = lat + det_lat_lng * ns_idx
            new_lng = lng + det_lat_lng * ew_idx
            results_ = gurunabiapi.get_results(new_lat, new_lng)
            if results_ == []:
                continue
            results.extend(results_)
            print("results_ length: ", len(results_))
            print("results length: ", len(results))
            print("store num: ", gurunabiapi.store_num)
            print("----------------")

    print("total store num: ",  gurunabiapi.store_num)

    df = pd.DataFrame(results, columns=gurunabiapi.columns)

    df.to_csv("./save_gurunabi_around_tokyo.csv")
    print("database shape: ", df.shape)