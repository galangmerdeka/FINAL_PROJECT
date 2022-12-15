import pandas as pd


class ConvertProcess():
    def convertZipsProcess(dataZips):
        length_data = len(dataZips)
        result_log = []
        for i in range(length_data):
    # if (i <= 5):
            zips_id = dataZips[i]["_id"]["$oid"]
            zips_city = dataZips[i]["city"]
            zips_zip = dataZips[i]["zip"]
            zips_pop = dataZips[i]["pop"]
            zips_state = dataZips[i]["state"]
            zips_loc_lat = dataZips[i]["loc"]["y"]
            zips_loc_long = dataZips[i]["loc"]["x"]
            # print(f"{i+1}. lat : ${zips_loc_lat}; long : ${zips_loc_long}")
            data_log = {
                    "id": zips_id,
                    "city": zips_city,
                    "zip": zips_zip,
                    "pop": zips_pop,
                    "state": zips_state,
                    "loc_lat": zips_loc_lat,
                    "loc_long": zips_loc_long
            }
            result_log.append(data_log)

        df = pd.DataFrame.from_dict(result_log)
        return df
    
    def convertCompaniesProcess(dataComps):
        length_data = len(dataComps)
        result_log = []
        for i in range(length_data):
            companies_id = dataComps[i]["_id"]["$oid"]
            name = dataComps[i]["name"]
            permalink = dataComps[i]["permalink"]
            crunchbase_url = dataComps[i]["crunchbase_url"]
            homepage_url = dataComps[i]["homepage_url"]
            blog_url = dataComps[i]["blog_url"]
            blog_feed_url = dataComps[i]["blog_feed_url"]
            twitter_username = dataComps[i]["twitter_username"]
            category_code = dataComps[i]["category_code"]
            number_of_employees = dataComps[i]["number_of_employees"]
            founded_year = dataComps[i]["founded_year"]
            founded_month = dataComps[i]["founded_month"]
            founded_day = dataComps[i]["founded_day"]
            deadpooled_year = dataComps[i]["deadpooled_year"]
            tag_list = dataComps[i]["tag_list"]
            email_address = dataComps[i]["email_address"]
            phone_number = dataComps[i]["phone_number"]
            description = dataComps[i]["description"]
            created_at = str(dataComps[i]["created_at"])
            updated_at = dataComps[i]["updated_at"]
            overview = dataComps[i]["overview"]
            total_money_raised = dataComps[i]["total_money_raised"]
            offices = dataComps[i]["offices"]
            for x in range(len(offices)):
                if(x == 0):
                    offices_description = offices[0]["description"]
                    offices_address1 = offices[0]["address1"]
                    offices_address2 = offices[0]["address2"]
                    offices_zip_code = offices[0]["zip_code"]
                    offices_city = offices[0]["city"]
                    offices_state_code = offices[0]["state_code"]
                    offices_country_code = offices[0]["country_code"]
                    offices_latitude = offices[0]["latitude"]
                    offices_longitude = offices[0]["longitude"]
            data_log = {
                "id" : companies_id,
                "name" : name,
                "permalink" : permalink,
                "crunchbase_url" : crunchbase_url,
                "homepage_url" : homepage_url,
                "blog_url" : blog_url,
                "blog_feed_url" : blog_feed_url,
                "twitter_username" : twitter_username,
                "category_code" : category_code,
                "number_of_employees" : number_of_employees,
                "founded_year" : founded_year,
                "founded_month" : founded_month,
                "founded_day" : founded_day,
                "deadpooled_year" : deadpooled_year,
                "tag_list" : tag_list,
                "email_address" : email_address,
                "phone_number" : phone_number,
                "description" : description,
                "created_at" : created_at,
                "updated_at" : updated_at,
                "overview" : overview,
                "total_money_raised" : total_money_raised,
                "offices_description" : offices_description,
                "offices_address1" : offices_address1,
                "offices_address2" : offices_address2,
                "offices_zipcode" : offices_zip_code,
                "offices_city" : offices_city,
                "offices_state_code" : offices_state_code,
                "offices_country_code" : offices_country_code,
                "offices_latitude" : offices_latitude,
                "offices_longitude" : offices_longitude
            }

            result_log.append(data_log)
        
        df = pd.DataFrame.from_dict(result_log)
        return df