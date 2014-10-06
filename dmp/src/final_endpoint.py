#!/usr/bin/python
# -*- coding: utf-8 -*-

import cherrypy
import pandas
import json
from datetime import datetime

config = {
  'global' : {
    'server.socket_host' : '0.0.0.0',
    'server.socket_port' : 9090,
    'server.threadPool' : 20,
  }
}

class Search(object):
    
    def __init__(self):
        cherrypy.log("[OK] load database")
        self.db = pandas.read_pickle('ga_psa_201409w1.pkl')
    
    @cherrypy.expose
    def index(self, **params):
        # set up response header
        cherrypy.response.headers['Content-Type'] = 'application/javascript'
        # read the expected headers list
        f = open("headers", "r")
        line = f.readline()
        headers = line[:-1].split(",")
        f.close()
        # get all the key/value pairs from the request and check that all keys belong to the expected headers
        keys_vals = cherrypy.request.params
        keys = keys_vals.keys()
        if not set(keys).issubset(set(headers)):
            cherrypy.log("[NOK] headers error")
            cherrypy.log(str([x for x in keys if x not in headers]))
            success = False
            code = 500
            result = json.dumps({"success": success, "code": code, "result": {"message": 'Parameters don\'t match expected headers'}})
            if 'callback' in params.keys():
                return '%s(%s)' % (params['callback'], result)
            else:
                cherrypy.response.headers['Content-Type'] = 'application/json'
                return result
        else:
            num_fields = ['sessionCount', 'daysSinceLastSession', 'vehiclePriceFinal', 'vehicleKm']
            reserved_fields = ['callback', '_']
            date_fields = ['uiBirthday_date1', 'uiBirthday_date2']
            listed_fields = ['deviceCategory', 'userType', 'newsletterOptin', 'uiExpectedPurchase', 'uiLogged', 'uiGender', 'vehicleFuel']
            keys_vals_part1 = {k:[v] for (k,v) in keys_vals.items() if isinstance(v, basestring) and k not in num_fields + reserved_fields + listed_fields + date_fields}
            keys_vals_part2 = {k:v for (k,v) in keys_vals.items() if not isinstance(v, basestring) and k not in num_fields + reserved_fields + listed_fields + date_fields}
            keys_vals_part3 = {k:map(float, v) for (k,v) in keys_vals.items() if k in listed_fields}
            keys_vals_part1.update(keys_vals_part2)
            keys_vals_part1.update(keys_vals_part3)
            qry = ""
            num_field_error = False
            for f in set(num_fields).intersection(set(keys_vals)):
                if keys_vals[f].find("-") < 0:
                    num_field_error = True
                    break
                rge = keys_vals[f].split('-')
                m = rge[0]
                M = rge[1]
                if f == "uiBirthday":
                    M = datetime.strptime(M, "%d/%m/%Y").strftime("%m/%d/%Y")
                    m = datetime.strptime(m, "%d/%m/%Y").strftime("%m/%d/%Y")
                    qry += f + " >= '" + m + "' and " + f + " <= '" + M + "' and "
                else:
                    qry += f + " >= " + m + " and " + f + " <= " + M + " and "
            qry += " and ".join([k + " in " + str(v) for (k,v) in keys_vals_part1.items()])
            if 'uiBirthday_date1' in keys_vals.keys() and len(keys_vals['uiBirthday_date1'])>0:
                qry += "and uiBirthday >= '" + datetime.strptime(keys_vals['uiBirthday_date1'], "%d/%m/%Y").strftime("%m/%d/%Y")  + "'"
            if 'uiBirthday_date2' in keys_vals.keys() and len(keys_vals['uiBirthday_date2'])>0:
                qry += "and uiBirthday <= '" + datetime.strptime(keys_vals['uiBirthday_date2'], "%d/%m/%Y").strftime("%m/%d/%Y")  + "'"    
            # remove last/first 'and' if needeed
            if qry.split()[-1] == "and": 
                qry = qry.split(' ', 1)[0]
            if qry.split()[0] == "and": 
                qry = qry.split(' ', 1)[1]
            if num_field_error:
                cherrypy.log("[NOK] error when parsing numeric fields")
                success = False
                code = 500
                result = json.dumps({"success": success, "code": code, "result": {"message": 'A range is expected for the following fields' + str(num_fields)}})
                if 'callback' in params.keys():
                    return '%s(%s)' % (params['callback'], result)
                else:
                    cherrypy.response.headers['Content-Type'] = 'application/json'
                    return result
            else:
                users = self.db.query(qry)
                nusers = users['key_inter_day'].unique().shape[0]
                total = self.db['key_inter_day'].unique().shape[0]
                pct = 100 * round(nusers/float(total), 2)
                cherrypy.log("[OK] a proper query has been provided : [" + qry + "] " + str(nusers) + " users have been found")
                success = True
                code = 200
                result = json.dumps({"success": success, "code": code, "result": {"percentage": pct, "count": nusers, "total": total}})
                if 'callback' in params.keys():
                    return '%s(%s)' % (params['callback'], result)
                else:
                    cherrypy.response.headers['Content-Type'] = 'application/json'
                    return result            
    
if __name__ == '__main__':
    cherrypy.quickstart(Search(), '/users', config = config)
    
