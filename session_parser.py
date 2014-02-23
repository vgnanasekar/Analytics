#------------------------------------------------------------------
#Main File for parsing the xml sessions file
#------------------------------------------------------------------

#Imports
from lxml import etree, objectify
from datetime import datetime
 
#Class to store session data
class Session(object):
    def __init__(self):
        self.session_no         = 0
        self.session_start_time = ''
        self.topic_num          = 0
        self.product            = ''
        self.goal               = ''
        self.tasktype           = ''
        self.subject_num        = ''
        self.subject            = ''
        self.interaction_count  = 0
        self.interactions       = []
        self.curr_query_stime   = 0
        self.curr_query         = ''

#Class to store Interaction data
class Interaction(object):
    def __init__(self, 
            intxn_start_time=   0,
            avg_click_time  =   0,
            click_count     =   0,
            clicks          =   ''
            ):
        self.intxn_start_time   = intxn_start_time
        self.avg_click_time     = avg_click_time
        self.click_count        = click_count
        self.clicks             = clicks

#----------------------------------------------------------------
#Defn   :   Calculates the time difference in milliseconds 
#Input  :   Two timestamps of format %H:%M:%S.%f
#Output :   Milliseconds as integer
#----------------------------------------------------------------
def timediff_ms(t1, t2):
    ta = datetime.strptime(t1, "%H:%M:%S.%f")
    tb = datetime.strptime(t2, "%H:%M:%S.%f")
    tams = ta.microsecond + ((ta.hour * 3600) + (ta.minute * 60) + ta.second) * 1000000  
    tbms = tb.microsecond + ((tb.hour * 3600) + (tb.minute * 60) + tb.second) * 1000000
    ms = abs(tams - tbms)
    return ms

#-------------------------------------------------------------------
#Defn   :   Converts Session data into csv strings and write to file
#Input  :   Sessions array
#Output :   Nothing(Write to file)
#-------------------------------------------------------------------
def convert_to_csv(sessions_a): 
    count = 0
    slen = len(sessions_a)
    ofile = open('output.csv', 'w')
    header = "interaction, session, ses start, topic no, product, "\
                "goal, tasttype, subject no, subject, "\
                "currquery start, intxn count, intxn start time, "\
                "avg click time, clicks, click order" 
    print >> ofile, header
    for cs in sessions_a:
        interactions_a = cs.interactions
        for ci in interactions_a:
            count += 1
            outstr = count, cs.session_no, cs.session_start_time, \
                    cs.topic_num, cs.product, cs.goal, cs.tasktype, \
                    cs.subject_num, cs.subject, cs.curr_query_stime, \
                    cs.interaction_count, ci.intxn_start_time, \
                    ci.avg_click_time, ci.click_count, ci.clicks

            outstr = [str(i) for i in outstr]
            print >> ofile, ','.join(outstr)
#End convert_to_csv

#----------------------------------------------------------------------
#Defn   :   Calculates the time difference in milliseconds 
#Input  :   Two timestamps of format %H:%M:%S.%f
#Output :   Milliseconds as integer
#----------------------------------------------------------------------
def parseXML(xmlFile):

    #Stores entire xml file as a string
    with open(xmlFile) as f:
        xml = f.read()
 
    #Convert the xml string into Objects in tree model.
    root = objectify.fromstring(xml)

    #For Storing the each session
    sessions_a = []

    for ses in root.session:
        #Create object for Current session
        cs = Session()
        
        #Array to store the interaction objects for the session
        interactions_a = []

        #Get Attribute data of session and topic
        ses_attr_h      = ses.attrib
        topic_attr_h    = ses.topic.attrib
        subject_attr_h  = ses.topic.subject.attrib

        #Storing sessions data -- Use A funtcion instead
        cs.session_no           = ses_attr_h['num']
        cs.session_start_time   = ses_attr_h['starttime']
        cs.topic_num            = topic_attr_h['num']
        cs.product              = topic_attr_h['product']
        cs.goal                 = topic_attr_h['goal']
        cs.tasktype             = topic_attr_h['tasktype']
        cs.subject_num          = subject_attr_h['num']
        cs.subject              = ses.topic.subject.text
        cs.curr_query           = ses.currentquery.query.text
        cs.curr_query_stime     = ses.currentquery.attrib['starttime']
        cs.interaction_count    = len(ses.interaction) 

        #Iterating through Each Session
        for inter in ses.interaction:
            #Object For every new interaction

            total_click_time = 0.0 
            avg_click_time = 0
            clicks = ''
            interaction_start_time = inter.attrib['starttime']

            for b in inter.clicked: 
                if (b):
                    click_count = len(b.click)
                    for click in b.click: 
                        rank    = click.rank.text
                        clicks += rank + '.'

                        #Attribute Holder of clicks
                        click_attr_h    = click.attrib 
                        start_time      = click_attr_h['starttime'] 
                        end_time        = click_attr_h['endtime'] 
                        total_click_time += timediff_ms(end_time, start_time)
                if (total_click_time > 0.0):
                    avg_click_time = total_click_time / click_count
            clicks = clicks.strip('.')
            ci = Interaction(interaction_start_time,
                                avg_click_time, 
                                click_count,
                                clicks)
            interactions_a.append(ci)
        cs.interactions = interactions_a
        sessions_a.append(cs)
    return sessions_a
#End parseXML

#----------------------------------------------------------------------
#Main function
#----------------------------------------------------------------------
if __name__ == "__main__":
    xmlFileName = r'sample.xml'

    sessions_a  = parseXML(xmlFileName)

    convert_to_csv(sessions_a)
