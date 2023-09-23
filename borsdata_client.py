# importing the borsdata_api
from borsdata_api import BorsdataAPI
from excel_test import ExcelWriter
# pandas is a data-analysis library for python (data frames)
import pandas as pd
# matplotlib for visual-presentations (plots)
import matplotlib.pylab as plt
# datetime for date- and time-stuff
import datetime as dt
# user constants
import constants as constants
import numpy as np
import os

# pandas options for string representation of data frames (print)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class BorsdataClient:
    def __init__(self):
        self._borsdata_api = BorsdataAPI(constants.API_KEY)
        self._instruments_with_meta_data = pd.DataFrame()

    def instruments_with_meta_data(self):
        """
        creating a csv and xlsx of the APIs instrument-data (including meta-data)
        and saves it to path defined in constants (default ../file_exports/)
        :return: pd.DataFrame of instrument-data with meta-data
        """
        if len(self._instruments_with_meta_data) > 0:
            return self._instruments_with_meta_data
        else:
            self._borsdata_api = BorsdataAPI(constants.API_KEY)
            # fetching data from api
            countries = self._borsdata_api.get_countries()
            branches = self._borsdata_api.get_branches()
            sectors = self._borsdata_api.get_sectors()
            markets = self._borsdata_api.get_markets()
            instruments = self._borsdata_api.get_instruments()
            # instrument type dict for conversion (https://github.com/Borsdata-Sweden/API/wiki/Instruments)
            instrument_type_dict = {0: 'Aktie', 1: 'Pref', 2: 'Index', 3: 'Stocks2', 4: 'SectorIndex',
                                    5: 'BranschIndex', 8: 'SPAC', 13: 'Index GI'}
            # creating an empty dataframe
            instrument_df = pd.DataFrame()
            # loop through the whole dataframe (table) i.e. row-wise-iteration.
            for index, instrument in instruments.iterrows():
                ins_id = index
                name = instrument['name']
                ticker = instrument['ticker']
                isin = instrument['isin']
                # locating meta-data in various ways
                # dictionary-lookup
                instrument_type = instrument_type_dict[instrument['instrument']]
                # .loc locates the rows where the criteria (inside the brackets, []) is fulfilled
                # located rows (should be only one) get the column 'name' and return its value-array
                # take the first value in that array ([0], should be only one value)
                market = markets.loc[markets.index == instrument['marketId']]['name'].values[0]
                country = countries.loc[countries.index == instrument['countryId']]['name'].values[0]
                sector = 'N/A'
                branch = 'N/A'
                # index-typed instruments does not have a sector or branch
                if market.lower() != 'index':
                    sector = sectors.loc[sectors.index == instrument['sectorId']]['name'].values[0]
                    branch = branches.loc[branches.index == instrument['branchId']]['name'].values[0]
                # appending current data to dataframe, i.e. adding a row to the table.
                df_temp = pd.DataFrame([{'name': name, 'ins_id': ins_id, 'ticker': ticker, 'isin': isin,
                                         'instrument_type': instrument_type,
                                         'market': market, 'country': country, 'sector': sector, 'branch': branch}])
                instrument_df = pd.concat([instrument_df, df_temp], ignore_index=True)
            """
            # create directory if it do not exist
            if not os.path.exists(constants.EXPORT_PATH):
                os.makedirs(constants.EXPORT_PATH)
            # to csv
            instrument_df.to_csv(constants.EXPORT_PATH + 'instrument_with_meta_data.csv')
            # creating excel-document
            excel_writer = pd.ExcelWriter(constants.EXPORT_PATH + 'instrument_with_meta_data.xlsx')
            # adding one sheet
            instrument_df.to_excel(excel_writer, 'instruments_with_meta_data')
            # saving the document
            excel_writer.save()
            """
            self._instruments_with_meta_data = instrument_df
            return instrument_df

    def plot_stock_prices(self, ins_id):
        """
        Plotting a matplotlib chart for ins_id
        :param ins_id: instrument id to plot
        :return:
        """
        # creating api-object
        # using api-object to get stock prices from API
        stock_prices = self._borsdata_api.get_instrument_stock_prices(ins_id)
        # calculating/creating a new column named 'sma50' in the table and
        # assigning the 50 day rolling mean to it
        stock_prices['sma50'] = stock_prices['close'].rolling(window=50).mean()
        # filtering out data after 2015 for plot
        filtered_data = stock_prices[stock_prices.index > dt.datetime(2015, 1, 1)]
        # plotting 'close' (with 'date' as index)
        plt.plot(filtered_data['close'], color='blue', label='close')
        # plotting 'sma50' (with 'date' as index)
        plt.plot(filtered_data['sma50'], color='black', label='sma50')
        # show legend
        plt.legend()
        # show plot
        plt.show()

    def top_performers(self, market, country, number_of_stocks=5, percent_change=1):
        """
        function that prints top performers for given parameters in the terminal
        :param market: which market to search in e.g. 'Large Cap'
        :param country: which country to search in e.g. 'Sverige'
        :param number_of_stocks: number of stocks to print, default 5 (top5)
        :param percent_change: number of days for percent change calculation
        :return: pd.DataFrame
        """
        # creating api-object
        # using defined function above to retrieve dataframe of all instruments
        instruments = self.instruments_with_meta_data()
        # filtering out the instruments with correct market and country
        filtered_instruments = instruments.loc[(instruments['market'] == market) & (instruments['country'] == country)]
        # creating new, empty dataframe
        stock_prices = pd.DataFrame()
        # looping through all rows in filtered dataframe
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_stock_price = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
            instrument_stock_price.sort_index(inplace=True)
            # calculating the current instruments percent change
            instrument_stock_price['pct_change'] = instrument_stock_price['close'].pct_change(percent_change)
            # getting the last row of the dataframe, i.e. the last days values
            last_row = instrument_stock_price.iloc[[-1]]
            # appending the instruments name and last days percent change to new dataframe
            df_temp = pd.DataFrame([{'stock': instrument['name'], 'pct_change': round(last_row['pct_change'].values[0] * 100, 2)}])
            stock_prices = pd.concat([stock_prices, df_temp], ignore_index=True)
        # printing the top sorted by pct_change-column
        print(stock_prices.sort_values('pct_change', ascending=False).head(number_of_stocks))
        return stock_prices

    def market_breadth_50(self, market):
        """
        function that prints breadth of specified market and cuntry
        :param market: which market to search in e.g. 'Large Cap'
        """
        # creating api-object
        # using defined function above to retrieve dataframe of all instruments
        instruments = self.instruments_with_meta_data()
        # filtering out the instruments with correct market
        filtered_instruments = instruments.loc[(instruments['market'] == market) & (instruments['country'] == 'Sverige')]
        # creating new, empty dataframe
        stock_prices = pd.DataFrame()
        # looping through all rows in filtered dataframe
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_stock_price = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
            instrument_stock_price.sort_index(inplace=True)
            # using numpy's where function to create a 1 if close > ma50, else a 0
            instrument_stock_price[f'above_ma50'] = np.where(
                instrument_stock_price['close'] > instrument_stock_price['close'].rolling(window=50).mean(), 1, 0)
            # getting the last row of the dataframe, i.e. the last days values
            last_row = instrument_stock_price.iloc[[-1]]
            # appending the instruments name and value to new dataframe
            df_temp = pd.DataFrame([{'stock': instrument['name'], 'above_ma50': last_row['above_ma50'].values[0]}])
            df_temp = df_temp.loc[(df_temp['above_ma50'] == 1)]
            stock_prices = pd.concat([stock_prices, df_temp], ignore_index=True)
            breadth = int(len(stock_prices)/len(filtered_instruments) * 100)
        print(breadth)
        return breadth

    def market_breadth(self, market):
        instruments = self._instruments_with_meta_data
        # filtering out the instruments with correct market
        filtered_instruments = instruments.loc[(instruments['market'] == market) & (instruments['country'] == 'Sverige')]
        # creating new, empty dataframe
        stock_prices = pd.DataFrame()
        stock_prices2 = pd.DataFrame()
        stock_prices3 = pd.DataFrame()
        # looping through all rows in filtered dataframe
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_stock_price = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
            instrument_stock_price.sort_index(inplace=True)
            # using numpy's where function to create a 1 if close > ma50, else a 0
            instrument_stock_price[f'above_ma50'] = np.where(
                instrument_stock_price['close'] > instrument_stock_price['close'].rolling(window=20).mean(), 1, 0)
            # getting the last row of the dataframe, i.e. the last days values
            last_row = instrument_stock_price.iloc[[-1]]
            # appending the instruments name and value to new dataframe
            df_temp = pd.DataFrame([{'stock': instrument['name'], 'above_ma50': last_row['above_ma50'].values[0]}])
            df_temp = df_temp.loc[(df_temp['above_ma50'] == 1)]
            stock_prices = pd.concat([stock_prices, df_temp], ignore_index=True)
            breadth = int(len(stock_prices)/len(filtered_instruments) * 100)
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_stock_price2 = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
            instrument_stock_price2.sort_index(inplace=True)
            # using numpy's where function to create a 1 if close > ma50, else a 0
            instrument_stock_price2[f'above_ma50'] = np.where(
                instrument_stock_price2['close'] > instrument_stock_price2['close'].rolling(window=50).mean(), 1, 0)
            # getting the last row of the dataframe, i.e. the last days values
            last_row2 = instrument_stock_price2.iloc[[-1]]
            # appending the instruments name and value to new dataframe
            df_temp2 = pd.DataFrame([{'stock': instrument['name'], 'above_ma50': last_row2['above_ma50'].values[0]}])
            df_temp2 = df_temp2.loc[(df_temp2['above_ma50'] == 1)]
            stock_prices2 = pd.concat([stock_prices2, df_temp2], ignore_index=True)
            breadth2 = int(len(stock_prices2)/len(filtered_instruments) * 100)
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_stock_price3 = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
            instrument_stock_price3.sort_index(inplace=True)
            # using numpy's where function to create a 1 if close > ma50, else a 0
            instrument_stock_price3[f'above_ma50'] = np.where(
                instrument_stock_price3['close'] > instrument_stock_price3['close'].rolling(window=200).mean(), 1, 0)
            # getting the last row of the dataframe, i.e. the last days values
            last_row3 = instrument_stock_price3.iloc[[-1]]
            # appending the instruments name and value to new dataframe
            df_temp3 = pd.DataFrame([{'stock': instrument['name'], 'above_ma50': last_row3['above_ma50'].values[0]}])
            df_temp3 = df_temp3.loc[(df_temp3['above_ma50'] == 1)]
            stock_prices3 = pd.concat([stock_prices3, df_temp3], ignore_index=True)
            breadth3 = int(len(stock_prices3)/len(filtered_instruments) * 100)
        return [breadth, breadth2, breadth3]

    def market_breadth_to_excel(self):
        self.instruments_with_meta_data()
        large_cap = self.market_breadth('Large Cap')
        mid_cap = self.market_breadth('Mid Cap')
        small_cap = self.market_breadth('Small Cap')
        first_north = self.market_breadth('First North')
        excel_export=ExcelWriter(large_cap, mid_cap, small_cap, first_north)
        excel_export.export_file()
        
    def history_kpi(self, kpi, market, country, year):
        """
        gathers and concatenates historical kpi-values for provided kpi, market and country
        :param kpi: kpi id see https://github.com/Borsdata-Sweden/API/wiki/KPI-History
        :param market: market to gather kpi-values from
        :param country: country to gather kpi-values from
        :param year: year for terminal print of kpi-values
        :return: pd.DataFrame of historical kpi-values
        """
        # creating api-object
        # using defined function above to retrieve data frame of all instruments
        instruments = self.instruments_with_meta_data()
        # filtering out the instruments with correct market and country
        filtered_instruments = instruments.loc[(instruments['market'] == market) & (instruments['country'] == country)]
        # creating empty array (to hold data frames)
        frames = []
        # looping through all rows in filtered data frame
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_kpi_history = self._borsdata_api.get_kpi_history(int(instrument['ins_id']), kpi, 'year', 'mean')
            # check to see if response holds any data.
            if len(instrument_kpi_history) > 0:
                # resetting index and adding name as a column
                instrument_kpi_history.reset_index(inplace=True)
                instrument_kpi_history.set_index('year', inplace=True)
                instrument_kpi_history['name'] = instrument['name']
                # appending data frame to array
                frames.append(instrument_kpi_history.copy())
        # creating concatenated data frame with concat
        symbols_df = pd.concat(frames)
        # the data frame has the columns ['year', 'period', 'kpi_value', 'name']
        # show year ranked from highest to lowest, show top 5
        print(symbols_df[symbols_df.index == year].sort_values('kpiValue', ascending=False).head(5))
        return symbols_df
    
    def get_latest_pe(self, ins_id):
            """
            Prints the PE-ratio of the provided instrument id
            :param ins_id: ins_id which PE-ratio will be calculated for
            :return:
            """
            # creating api-object
            # fetching all instrument data
            reports_quarter, reports_year, reports_r12 = self._borsdata_api.get_instrument_reports(3)
            # getting the last reported eps-value
            reports_r12.sort_index(inplace=True)
            #print(reports_r12.tail())
            last_eps = reports_r12['earningsPerShare'].values[-1]
            print(last_eps)
            # getting the stock prices
            stock_prices = self._borsdata_api.get_instrument_stock_prices(ins_id)
            stock_prices.sort_index(inplace=True)
            # getting the last close
            last_close = stock_prices['close'].values[-1]
            # getting the last date
            last_date = stock_prices.index.values[-1]
            # getting instruments data to retrieve the name of the ins_id
            instruments = self._borsdata_api.get_instruments()
            instrument_name = instruments[instruments.index == ins_id]['name'].values[0]
            print(instrument_name)
            # printing the name and calculated PE-ratio with the corresponding date. (array slicing, [:10])
            #print(f"PE for {instrument_name} is {round(last_close / last_eps, 1)} with data from {str(last_date)[:10]}")

    def get_eps_accelerationR12(self):
        instruments = self.instruments_with_meta_data()
        # filtering out the instruments with correct market and country
        #filtered_instruments = instruments.loc[(instruments['country'] == 'Finland') | (instruments['country'] == 'Sverige') | (instruments['country'] == 'Norge') | (instruments['country'] == 'Danmark') & (instruments['market'] != 'Spotlight') & (instruments['market'] != 'NGM') & (instruments['market'] != 'PepMarket')]
        filtered_instruments = instruments.loc[(instruments['market'] != 'Spotlight') & (instruments['market'] != 'NGM') & (instruments['market'] != 'PepMarket') & (instruments['country'] == 'Sverige')]
        #print(filtered_instruments['ins_id'])
        results_df = pd.DataFrame(columns=['instrument_name', 'average_3period_epsgrowth'])
        for index, instrument in filtered_instruments.iterrows():
             
            reports_quarter, reports_year, reports_r12 = self._borsdata_api.get_instrument_reports(int(instrument['ins_id']))
            if len(reports_r12) >= 7:
                reports_r12.sort_index(inplace=True)
                r12_diff= ((reports_r12['earningsPerShare'].values[-1]-reports_r12['earningsPerShare'].values[-5])/reports_r12['earningsPerShare'].values[-5])*100
                previous_r12_diff = ((reports_r12['earningsPerShare'].values[-2]-reports_r12['earningsPerShare'].values[-6])/reports_r12['earningsPerShare'].values[-6])*100
                previous_previous_r12_fidd = ((reports_r12['earningsPerShare'].values[-3]-reports_r12['earningsPerShare'].values[-7])/reports_r12['earningsPerShare'].values[-7])*100

                if (r12_diff > previous_r12_diff > previous_previous_r12_fidd > 0) & (reports_r12['earningsPerShare'].values[-7] > 0) & (reports_r12['earningsPerShare'].values[-6] > 0) & (reports_r12['earningsPerShare'].values[-5] > 0):


                    instruments = self._borsdata_api.get_instruments()
                    instrument_name = instruments[instruments.index == int(instrument['ins_id'])]['name'].values[0]
                    results_df = results_df.append({'instrument_name': instrument_name, 'average_3period_epsgrowth': int(round((r12_diff+previous_r12_diff+previous_previous_r12_fidd)/3, 0))}, ignore_index=True)
            else:
                pass
        #return results_df     
        print(results_df.sort_values(by=['average_3period_epsgrowth'], ascending=False))

    def get_eps_accelerationQ(self):
        instruments = self.instruments_with_meta_data()
        #filtered_instruments = instruments.loc[(instruments['market'] != 'Spotlight') & (instruments['market'] != 'NGM') & (instruments['market'] != 'PepMarket') & (instruments['country'] == 'Sverige')]
        filtered_instruments = instruments.loc[(instruments['country'] == 'Finland') | (instruments['country'] == 'Sverige') | (instruments['country'] == 'Norge') | (instruments['country'] == 'Danmark') & (instruments['market'] != 'Spotlight') & (instruments['market'] != 'NGM') & (instruments['market'] != 'PepMarket')]

        results_df = pd.DataFrame(columns=['instrument_name', 'average_3period_epsgrowth'])
        for index, instrument in filtered_instruments.iterrows():
             
            reports_quarter, reports_year, reports_r12 = self._borsdata_api.get_instrument_reports(int(instrument['ins_id']))
            if len(reports_quarter) >= 7:
                reports_quarter.sort_index(inplace=True)
                r12_diff= ((reports_quarter['earningsPerShare'].values[-1]-reports_quarter['earningsPerShare'].values[-5])/reports_quarter['earningsPerShare'].values[-5])*100
                previous_r12_diff = ((reports_quarter['earningsPerShare'].values[-2]-reports_quarter['earningsPerShare'].values[-6])/reports_quarter['earningsPerShare'].values[-6])*100
                previous_previous_r12_fidd = ((reports_quarter['earningsPerShare'].values[-3]-reports_quarter['earningsPerShare'].values[-7])/reports_quarter['earningsPerShare'].values[-7])*100

                if (r12_diff > previous_r12_diff > previous_previous_r12_fidd > 0) & (reports_quarter['earningsPerShare'].values[-7] > 0) & (reports_quarter['earningsPerShare'].values[-6] > 0) & (reports_quarter['earningsPerShare'].values[-5] > 0):


                    instruments = self._borsdata_api.get_instruments()
                    instrument_name = instruments[instruments.index == int(instrument['ins_id'])]['name'].values[0]
                    results_df = results_df.append({'instrument_name': instrument_name, 'average_3period_epsgrowth': int(round((r12_diff+previous_r12_diff+previous_previous_r12_fidd)/3, 0))}, ignore_index=True)
            else:
                pass
        #return results_df     
        print(results_df.sort_values(by=['average_3period_epsgrowth'], ascending=False)) 

    def get_eps_growth(self):
        """
        
        """
        instruments = self.instruments_with_meta_data()
        # filtering out the instruments with correct market and country
        filtered_instruments = instruments.loc[(instruments['market'] == 'Large Cap') & (instruments['country'] == 'Sverige')]
        #print(filtered_instruments['ins_id'])
        results_df = pd.DataFrame(columns=['instrument_name', 'current_quarter_R12', 'previous_quarter_R12', 'quarter_on_quarter'])
        for index, instrument in filtered_instruments.iterrows():
             
            reports_quarter, reports_year, reports_r12 = self._borsdata_api.get_instrument_reports(int(instrument['ins_id']))
            if len(reports_r12) >= 6:
                reports_r12.sort_index(inplace=True)
                r12_diff= ((reports_r12['earningsPerShare'].values[-1]-reports_r12['earningsPerShare'].values[-5])/reports_r12['earningsPerShare'].values[-5])*100
                previous_r12_diff = ((reports_r12['earningsPerShare'].values[-2]-reports_r12['earningsPerShare'].values[-6])/reports_r12['earningsPerShare'].values[-6])*100
                q_on_q_diff = ((reports_r12['earningsPerShare'].values[-1]-reports_r12['earningsPerShare'].values[-2])/reports_r12['earningsPerShare'].values[-2])*100
             
                instruments = self._borsdata_api.get_instruments()
                instrument_name = instruments[instruments.index == int(instrument['ins_id'])]['name'].values[0]
                results_df = results_df.append({'instrument_name': instrument_name, 'current_quarter_R12': int(round(r12_diff, 0)), 'previous_quarter_R12': int(round(previous_r12_diff, 0)), 'quarter_on_quarter': int(round(q_on_q_diff, 0))}, ignore_index=True)
            else:
                pass

        print(results_df.sort_values(by=['current_quarter_R12'], ascending=False))

    def breadth_large_cap_sweden(self):
        """
        plots the breadth (number of stocks above moving-average 40) for Large Cap Sweden compared
        to Large Cap Sweden Index
        """
        # creating api-object
        # using defined function above to retrieve data frame of all instruments
        instruments = self.instruments_with_meta_data()
        # filtering out the instruments with correct market and country
        filtered_instruments = instruments.loc[
            (instruments['market'] == "Large Cap") & (instruments['country'] == "Sverige")]
        # creating empty array (to hold data frames)
        frames = []
        # looping through all rows in filtered data frame
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_stock_prices = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
            # using numpy's where function to create a 1 if close > ma40, else a 0
            instrument_stock_prices[f'above_ma40'] = np.where(
                instrument_stock_prices['close'] > instrument_stock_prices['close'].rolling(window=40).mean(), 1, 0)
            instrument_stock_prices['name'] = instrument['name']
            # check to see if response holds any data.
            if len(instrument_stock_prices) > 0:
                # appending data frame to array
                frames.append(instrument_stock_prices.copy())
        # creating concatenated data frame with concat
        symbols_df = pd.concat(frames)
        symbols_df = symbols_df.groupby('date').sum()
        # fetching OMXSLCPI data from api
        omx = self._borsdata_api.get_instrument_stock_prices(643)
        # aligning data frames
        omx = omx[omx.index > '2015-01-01']
        symbols_df = symbols_df[symbols_df.index > '2015-01-01']
        # creating subplot
        fig, (ax1, ax2) = plt.subplots(2, sharex=True)
        # plotting
        ax1.plot(omx['close'], label="OMXSLCPI")
        ax2.plot(symbols_df[f'above_ma40'], label="number of stocks above ma40")
        # show legend
        ax1.legend()
        ax2.legend()
        plt.show()

    def branch_breadth(self):
        instruments = self.instruments_with_meta_data()
        #filtered_instruments = instruments.loc[(instruments['market'] != 'Spotlight') & (instruments['market'] != 'NGM') & (instruments['market'] != 'PepMarket') & (instruments['country'] == 'Sverige')]
        filtered_instruments = instruments.loc[(instruments['country'] == 'Finland') | (instruments['country'] == 'Sverige') | (instruments['country'] == 'Norge') | (instruments['country'] == 'Danmark')]

        stock_prices = pd.DataFrame()
        branch_breadth = pd.DataFrame(columns=['Bransch', '% > MA20', '% > MA50', '% > MA200', 'Antal Bolag'])
        my_list = ['Olja & Gas - Borrning', 'Olja & Gas - Exploatering', 'Olja & Gas - Transport', 'Olja & Gas - Försäljning', 'Olja & Gas - Service', 'Bränsle - Kol', 'Bränsle - Uran', 'Elförsörjning', 'Gasförsörjning', 'Vattenförsörjning', 'Förnybarenergi', 'Vindkraft', 'Solkraft', 'Bioenergi', 'Kemikalier', 'Gruv - Prospekt & Drift', 'Gruv - Industrimetaller', 'Gruv - Guld & Silver', 'Gruv - Ädelstenar', 'Gruv - Service', 'Skogsbolag', 'Förpackning', 'Industrimaskiner', 'Industrikomponenter', 'Elektroniska komponenter', 'Militär & Försvar', 'Energi & Återvinning', 'Byggnation & Infrastruktur', 'Bostadsbyggnation', 'Installation & VVS', 'Byggmaterial', 'Bygginredning', 'Bemanning', 'Affärskonsulter', 'Säkerhet', 'Utbildning', 'Stödtjänster & Service', 'Mätning & Analys', 'Information & Data', 'Flygtransport', 'Sjöfart & Rederi', 'Tåg- & Lastbilstransport', 'Kläder & Skor', 'Accessoarer', 'Hemelektronik', 'Möbler & Inredning', 'Fritid & Sport', 'Bil & Motor', 'Konsumentservice', 'Detaljhandel', 'Hotell & Camping', 'Restaurang & Café', 'Resor & Nöjen', 'Betting & Casino', 'Gaming & Spel', 'Marknadsföring', 'Media & Publicering', 'Bryggeri', 'Drycker', 'Jordbruk', 'Fiskodling', 'Tobak', 'Livsmedel', 'Hygienprodukter', 'Hälsoprodukter', 'Apotek', 'Livsmedelsbutiker', 'Banker', 'Nischbanker', 'Kredit & Finansiering', 'Kapitalförvaltning', 'Fondförvaltning', 'Investmentbolag', 'Försäkring', 'Fastighetsbolag', 'Fastighet - REIT', 'Läkemedel', 'Biotech', 'Medicinsk Utrustning', 'Hälsovård & Hjälpmedel', 'Sjukhus & Vårdhem', 'Elektronik & Tillverkning', 'Datorer & Hårdvara', 'Elektronisk Utrustning', 'Biometri', 'Kommunikation', 'Rymd- & Satellitteknik', 'Säkerhet & Bevakning', 'IT-Konsulter', 'Affärs- & IT-System', 'Internettjänster', 'Betalning & E-handel', 'Bredband & Telefoni', 'Telekomtjänster']
        
        for branch in my_list:
            sector_instruments = filtered_instruments.loc[(instruments['branch'] == branch)]

            for index, instrument in sector_instruments.iterrows():
                # fetching the stock prices for the current instrument
                instrument_stock_price = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
                instrument_stock_price.sort_index(inplace=True)
                
                # using numpy's where function to create a 1 if close > ma20, else a 0
                instrument_stock_price[f'above_ma20'] = np.where(
                    instrument_stock_price['close'] > instrument_stock_price['close'].rolling(window=20).mean(), 1, 0)
                
                # using numpy's where function to create a 1 if close > ma50, else a 0
                instrument_stock_price[f'above_ma50'] = np.where(
                    instrument_stock_price['close'] > instrument_stock_price['close'].rolling(window=50).mean(), 1, 0)
                
                instrument_stock_price[f'above_ma200'] = np.where(
                    instrument_stock_price['close'] > instrument_stock_price['close'].rolling(window=200).mean(), 1, 0)
                
                # getting the last row of the dataframe, i.e. the last day's values
                last_row = instrument_stock_price.iloc[[-1]]
                
                # appending the instrument's name and value to new dataframe
                df_temp = pd.DataFrame([{'stock': instrument['name'], 'above_ma20': last_row['above_ma20'].values[0], 'above_ma50': last_row['above_ma50'].values[0], 'above_ma200': last_row['above_ma200'].values[0]}])
                
                stock_prices = pd.concat([stock_prices, df_temp], ignore_index=True)
            
            try:
                breadth20 = int(len(stock_prices[stock_prices['above_ma20'] == 1])/len(sector_instruments) * 100)
            except:
                breadth20 = None
            
            try:
                breadth50 = int(len(stock_prices[stock_prices['above_ma50'] == 1])/len(sector_instruments) * 100)
            except:
                breadth50 = None
            
            try:
                breadth200 = int(len(stock_prices[stock_prices['above_ma200'] == 1])/len(sector_instruments) * 100)
            except:
                breadth200 = None
            
            branch_breadth = branch_breadth.append({'Bransch': branch, '% > MA20': breadth20, '% > MA50': breadth50, '% > MA200': breadth200, 'Antal Bolag': int(len(sector_instruments))}, ignore_index=True)
            
            df_temp= pd.DataFrame(None)
            stock_prices= pd.DataFrame(None)
        branch_breadth = branch_breadth.replace(to_replace='None', value=np.nan).dropna()
        print(branch_breadth.sort_values(by=['% > MA20'], ascending=False))

        branch_breadth.to_excel('file_exports/branch-breadth.xlsx', sheet_name='sheet 1', index=False)

    def sector_breadth(self):

        instruments = self.instruments_with_meta_data()
        #filtered_instruments = instruments.loc[(instruments['market'] != 'Spotlight') & (instruments['market'] != 'NGM') & (instruments['market'] != 'PepMarket') & (instruments['country'] == 'Sverige')]
        filtered_instruments = instruments.loc[(instruments['country'] == 'Finland') | (instruments['country'] == 'Sverige') | (instruments['country'] == 'Norge') | (instruments['country'] == 'Danmark')]
        stock_prices = pd.DataFrame()
        sector_breadth = pd.DataFrame(columns=['Sektor', '% > MA20', '% > MA50', '% > MA200'])
        
        my_list = ['Energi', 'Kraftförsörjning', 'Material', 'Dagligvaror', 'Sällanköpsvaror', 'Industri', 'Hälsovård', 'Finans & Fastighet', 'Informationsteknik', 'Telekommunikation']

        for sector in my_list:
            sector_instruments = filtered_instruments.loc[(instruments['sector'] == sector)]

            for index, instrument in sector_instruments.iterrows():
                # fetching the stock prices for the current instrument
                instrument_stock_price = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
                instrument_stock_price.sort_index(inplace=True)
                
                # using numpy's where function to create a 1 if close > ma20, else a 0
                instrument_stock_price[f'above_ma20'] = np.where(
                    instrument_stock_price['close'] > instrument_stock_price['close'].rolling(window=20).mean(), 1, 0)
                
                # using numpy's where function to create a 1 if close > ma50, else a 0
                instrument_stock_price[f'above_ma50'] = np.where(
                    instrument_stock_price['close'] > instrument_stock_price['close'].rolling(window=50).mean(), 1, 0)
                
                instrument_stock_price[f'above_ma200'] = np.where(
                    instrument_stock_price['close'] > instrument_stock_price['close'].rolling(window=200).mean(), 1, 0)
                
                # getting the last row of the dataframe, i.e. the last day's values
                last_row = instrument_stock_price.iloc[[-1]]
                
                # appending the instrument's name and value to new dataframe
                df_temp = pd.DataFrame([{'stock': instrument['name'], 'above_ma20': last_row['above_ma20'].values[0], 'above_ma50': last_row['above_ma50'].values[0], 'above_ma200': last_row['above_ma200'].values[0]}])
                
                stock_prices = pd.concat([stock_prices, df_temp], ignore_index=True)
            
            try:
                breadth20 = int(len(stock_prices[stock_prices['above_ma20'] == 1])/len(sector_instruments) * 100)
            except:
                breadth20 = None
            
            try:
                breadth50 = int(len(stock_prices[stock_prices['above_ma50'] == 1])/len(sector_instruments) * 100)
            except:
                breadth50 = None
            
            try:
                breadth200 = int(len(stock_prices[stock_prices['above_ma200'] == 1])/len(sector_instruments) * 100)
            except:
                breadth200 = None
            
            sector_breadth = sector_breadth.append({'Sektor': sector, '% > MA20': breadth20, '% > MA50': breadth50, '% > MA200': breadth200}, ignore_index=True)
            
            df_temp= pd.DataFrame(None)
            stock_prices= pd.DataFrame(None)

        sector_breadth = sector_breadth.replace(to_replace='None', value=np.nan).dropna()
        #print(sector_breadth.sort_values(by=['% > MA20'], ascending=False))
        sector_breadth.to_excel('file_exports/sector-breadth.xlsx', sheet_name='sheet 1', index=False)


if __name__ == "__main__":
    # Main, call functions here.
    # creating BorsdataClient-instance
    borsdata_client = BorsdataClient()
    # borsdata_client.get_eps_accelerationQ()
    # calling some methods
    #borsdata_client.instruments_with_meta_data()
    #borsdata_client.market_breadth2('Large Cap')
    #borsdata_client.market_breadth_to_excel()
    #borsdata_client.breadth_large_cap_sweden_number()
    #borsdata_client.breadth_large_cap_sweden()
    #borsdata_client.get_eps_acceleration()
    # borsdata_client.get_eps_accelerationR12()
    # borsdata_client.get_eps_accelerationQ()
    borsdata_client.sector_breadth()
    borsdata_client.branch_breadth()
    #borsdata_client.sector_breadth()
    #borsdata_client.plot_stock_prices(3)  # ABB
    #borsdata_client.history_kpi(2, 'Large Cap', 'Sverige', 2020)  # 2 == Price/Earnings (PE)
    #borsdata_client.top_performers('Large Cap', 'Sverige', 10, 5)  # showing top10 performers based on 5 day return (1 week) for Large Cap Sverige.
