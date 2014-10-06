import os, pandas, numpy                               

files = [f for f in os.listdir('.') if os.path.isfile(f)]
df_list = []
for f in files:
    print f
    df_list.append(pandas.read_csv(f, index_col=False, header=0))

df = pandas.concat(df_list)

df['date'] = pandas.to_datetime(df['date'])
df['uiBirthday'] = pandas.to_datetime(df['uiBirthday'])
df['daysSinceLastSession'] = df['daysSinceLastSession'].convert_objects(convert_numeric='force')
df['sessionCount'] = df['sessionCount'].convert_objects(convert_numeric='force')
df['vehicleKm'] = df['vehicleKm'].convert_objects(convert_numeric='force')
df['vehiclePriceFinal'] = df['vehiclePriceFinal'].convert_objects(convert_numeric='force')

correction = {'deviceCategory': {'desktop': '0', 'tablet': '1', 'mobile': '2'}}
df.replace(to_replace=correction, inplace=True)
df['deviceCategory'] = df['deviceCategory'].convert_objects(convert_numeric='force')

correction = {'userType': {'New Visitor': '0', 'Returning Visitor': '1'}}
df.replace(to_replace=correction, inplace=True)
df['userType'] = df['userType'].convert_objects(convert_numeric='force')

correction = {'newsletterOptin': {'0.0': '0', '1.0': '1'}}
df.replace(to_replace=correction, inplace=True)
df['newsletterOptin'] = df['newsletterOptin'].convert_objects(convert_numeric='force')

correction = {'uiExpectedPurchase': {'0.0': '0', 'LESS_THAN_1_MONTH': '0', '0 à 3 mois': '0', 'DPR_0': '0',
              'NEAR_3_MONTHS': '1', 'DPR_3': '1', '3 à 6 mois': '1', '1.0': '1',
              'NEAR_6_MONTHS': '2', 'DPR_6': '2', '6 mois à 1 an': '2', '2.0': '2',
              'MORE_THAN_6_MONTHS': '3', 'DPR_12': '3', 'Plus d\'1 an': '3',
              '4.0': '4'}}
df.replace(to_replace=correction, inplace=True)
df['uiExpectedPurchase'] = df['uiExpectedPurchase'].convert_objects(convert_numeric='force')

correction = {'uiLogged': {'1.0': '1', 'Y': '1', '0.0': '0', 'N': '0'}}
df.replace(to_replace=correction, inplace=True)
df['uiLogged'] = df['uiLogged'].convert_objects(convert_numeric='force')

correction = {'uiGender': {'Mademoiselle': '0', 'Mlle': '0', 'Madame': '1', 'Mme': '1', 'M.': '2', 'Monsieur': '2', 'Mr': '2'}}
df.replace(to_replace=correction, inplace=True)
df['uiGender'] = df['uiGender'].convert_objects(convert_numeric='force')

correction = {'vehicleFuel': {'Essence': '0', 'HDi': '1', 'Hybride-Diesel': '2'}}
df.replace(to_replace=correction, inplace=True)
df['vehicleFuel'][~df['vehicleFuel'].isin(correction['vehicleFuel'].values())] = numpy.nan
