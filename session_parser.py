#------------------------------------------------------------------
#Main File for parsing the xml sessions file
#------------------------------------------------------------------

#Imports
from lxml import etree, objectify
from datetime import datetime
from pprint import pprint
from copy import copy
import numpy


#Global storage
#------------------------------------------
            #Hashes
Results_h       =   {}
Sessions_h      =   {}
Interactions_h  =   {}
#------------------------------------------

#------------------------------------------
            #Array
UserStates_h    =   []
#------------------------------------------

#------------------------------------------
            #Variables
session_count   =   0
intxn_count     =   0
result_ofile    =   'Results.csv'
state_ofile     =   'states.csv'
state_count     =   0
MAXTOP_OLAP     =   5
#------------------------------------------
 
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
            result_count    =   0,
            clicks          =   '',
            clicks_a        =   []
            ):
        self.intxn_start_time   = intxn_start_time
        self.avg_click_time     = avg_click_time
        self.click_count        = click_count
        self.result_count       = result_count
        self.clicks             = clicks
        self.click_a            = clicks_a 

#Class to store click data
class Result(object):
    def __init__(self,
            rank,
            overlap_percent,
            url,
            title,
            snippet,
            query,
            interaction_num,
            session_num):
        self.rank               = rank
        self.overlap_percent    = overlap_percent
        self.url                = url
        self.title              = title
        self.snippet            = snippet
        self.query              = query
        self.interaction_num    = interaction_num
        self.session_num        = session_num
        self.clicked            = False
        self.start_time         = 0
        self.end_time           = 0

#Class defining states
class UserState(object):
    def __init__(self,
            start_state         =   1,
            state_entry_time    =   0,
            state_duration      =   0,
            total_duration      =   0,
            click_count         =   0,
            topresult_overlap   =   0,
            click_overlap       =   0,
            clicks_first_intxn  =   0,
            interaction_count   =   0,
            next_state          =   0):
        self.start_state        = start_state
        self.state_entry_time   = state_entry_time
        self.state_duration     = state_duration    
        self.total_duration     = total_duration
        self.click_count        = click_count   
        self.topresult_overlap  = topresult_overlap
        self.click_overlap      = click_overlap
        self.clicks_first_intxn = clicks_first_intxn
        self.interaction_count  = interaction_count
        self.next_state         = next_state


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

#----------------------------------------------------------------
#Write state data to csv 
#----------------------------------------------------------------
def write_state_data():
    global state_count, UserStates_h
    header  =   "State NO," \
                "Start State," \
                "Next State," \
                "#Interactions," \
                "State Duration," \
                "Total Duration," \
                "Click Count," \
                "Result Overlap, "\
                "Click Overlap," \
                "first intxn clicks"

    #Results file for writing every result date
    ofile = open(state_ofile, 'w')
    print >> ofile, header

    for sn in range(1, session_count+1):
        cs      = Sessions_h[sn]
        intxns  = cs.interaction_count
        
        #Start User state
        ustate  = UserState()
        ustate.next_state = 2
        UserStates_h.append(copy(ustate))
        start_time = cs.session_start_time
        for itxn in range(1, intxns+1):
            inter_obj   = Interactions_h[sn, itxn]
            ci          = inter_obj
            cc          = inter_obj.click_count

            #NRQ State update
            ustate.start_state      = ustate.next_state
            ustate.interaction_count= itxn
            ustate.state_entry_time = ci.intxn_start_time 
            ustate.next_state       = 3
            ustate.state_duration   = timediff_ms(ci.intxn_start_time, start_time)
            start_time              = ci.intxn_start_time
            ustate.total_duration          += ustate.state_duration
            UserStates_h.append(copy(ustate))

            #If no results continue
            if (inter_obj.result_count <= 0):
                continue

            #Result State update
            ustate.start_state      = ustate.next_state
            ustate.interaction_count= itxn
            ustate.state_entry_time = ci.intxn_start_time 
            ustate.next_state       = -1
            ustate.state_duration   = 0
            olap_a = []
            for rank in range(1, min(MAXTOP_OLAP+1, inter_obj.result_count+1)): 
                res = Results_h[sn, itxn, rank]
                res_olap_percent = query_overlap_percent(res.query,res.title, res.snippet)
                olap_a.append(res_olap_percent)
                
            ustate.topresult_overlap = numpy.mean(olap_a)


            #Convert the click order into integers
            all_results = range(1, inter_obj.result_count+1)
            clicked_results = []
            if (inter_obj.clicks):
                clicked_results = map(int, inter_obj.clicks.split('.'))
            unclicked_results   = list(set(all_results) - set(clicked_results))
            net_results         = clicked_results + unclicked_results

            for rank in net_results: 
                cc = Results_h[sn, itxn, rank]
                if (cc.clicked):
                    ustate.next_state        = 4
                    UserStates_h.append(copy(ustate))
                    ustate.start_state      = ustate.next_state
                    ustate.next_state       = -1
                    ustate.click_overlap    = cc.overlap_percent 
                    ustate.click_count      += 1
                    ustate.state_duration   = timediff_ms(cc.start_time, cc.end_time)
                    ustate.total_duration          += ustate.state_duration
                    start_time              = cc.end_time
                    
                 
        #End User state
        ustate.next_state       = 5
        UserStates_h.append(copy(ustate))
        ustate.start_state      = ustate.next_state
        ustate.next_state       = 5
        ustate.state_duration   = timediff_ms(cs.curr_query_stime, start_time) 
        ustate.total_duration          += ustate.state_duration
        UserStates_h.append(copy(ustate))
   
    for state_i in range(0, len(UserStates_h)):
        ustate   =   UserStates_h[state_i]
        outstr = state_i, \
                ustate.start_state, \
                ustate.next_state, \
                ustate.interaction_count, \
                ustate.state_duration, \
                ustate.total_duration, \
                ustate.click_count, \
                ustate.topresult_overlap, \
                ustate.click_overlap, \
                ustate.clicks_first_intxn
        outstr = [str(i) for i in outstr]
        print >> ofile, ','.join(outstr)
    

#----------------------------------------------------------------
#Write click data to a csv
#----------------------------------------------------------------
def write_click_data():
    #Results file for writing every result date
    ofile = open(result_ofile, 'w')
    header = "interaction, session, ses start, topic no, product, "\
                "goal, tasttype, subject no, subject, "\
                "currquery start, intxn count, intxn start time, "\
                "rank, clicked, overlap %, click start, click end, "\
                "url, query"
    print >> ofile, header
    for sn in range(1, session_count+1):
        cs      = Sessions_h[sn]
        intxns  = cs.interaction_count
        for itxn in range(1, intxns+1):
            inter_obj = Interactions_h[sn, itxn]
            ci = inter_obj
            cc = inter_obj.click_count
            #Convert the click order into integers
            if (inter_obj.result_count <= 0):
                continue
            all_results = range(1, inter_obj.result_count+1)
            clicked_results = []
            if (inter_obj.clicks):
                clicked_results = map(int, inter_obj.clicks.split('.'))
            unclicked_results = list(set(all_results) - set(clicked_results))
            net_results = clicked_results + unclicked_results

            for rank in net_results: 
                cc = Results_h[sn, itxn, rank]
                outstr = cc.session_num, \
                        cc.interaction_num, \
                        cs.session_start_time, \
                        cs.topic_num, \
                        cs.product, \
                        cs.goal, \
                        cs.tasktype, \
                        cs.subject_num, \
                        cs.subject, \
                        cs.curr_query_stime, \
                        cs.interaction_count, \
                        ci.intxn_start_time, \
                        cc.rank, \
                        cc.clicked, \
                        cc.overlap_percent, \
                        cc.start_time, \
                        cc.end_time, \
                        cc.url, \
                        cc.query

                outstr = [str(i) for i in outstr]
                print >> ofile, ','.join(outstr)

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

#Returns array of words from string
def getwords(instr):
    return [x.lower() for x in instr.split(' ')]

#Find overlap percent betwen query and title + snippet combination
def query_overlap_percent(query, title='', snippet=''):
    qwords          = getwords(query)
    combined_words  = getwords(title) + getwords(snippet)
    occured_words   = list(set(qwords) & set(combined_words))
    olap_percent = 0
    if (len(occured_words) > 1):
        olap_percent = float(float(len(occured_words)) / float(len(qwords))) * 100 
    return olap_percent

#----------------------------------------------------------------------
#Defn   :   Calculates the time difference in milliseconds 
#Input  :   Two timestamps of format %H:%M:%S.%f
#Output :   Milliseconds as integer
#----------------------------------------------------------------------
def parseXML(xmlFile):
    global session_count, intxn_count;
    global Results_h, Sessions_h, Interactions_h;

    #Stores entire xml file as a string
    with open(xmlFile) as f:
        xml = f.read()
 
    #Convert the xml string into Objects in tree model.
    root = objectify.fromstring(xml)

    #For Storing the each session
    sessions_a = []

    for ses in root.session:
        session_count += 1
        intxn_num = 0
        #Create object for Current session
        cs = Session()
        
        #Array to store the interaction objects for the session
        interactions_a = []

        #Get Attribute data of session and topic
        ses_attr_h      = ses.attrib
        topic_attr_h    = ses.topic.attrib
        subject_attr_h  = ses.topic.subject.attrib

        #Storing sessions data -- Use A funtcion instead
        cs.session_no           = int(ses_attr_h['num'])
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
        Sessions_h[session_count] = cs

        #Iterating through Each Session
        for inter in ses.interaction:
            total_click_time = 0.0 
            avg_click_time = 0
            clicks = ''
            interaction_start_time = inter.attrib['starttime']
            intxn_num = int(inter.attrib['num'])
            intxn_count += 1
            click_count = 0
            query = inter.query.text

            #Result update
            clicks_a = []
            result_count = 0
            for b in inter.results:
                if (not b):
                    continue
                result_count = len(inter.results.result)
                for res in b.result:
                    click_rank      = int(res.attrib['rank'])
                    click_url       = res.url.text
                    click_webid     = res.clueweb09id.text
                    click_title     = res.title.text
                    click_snippet   = res.snippet.text
                    click_olap_pcnt = query_overlap_percent(query, click_title, click_snippet) 
                    result_object       = Result(click_rank,
                                            click_olap_pcnt,
                                            click_url,
                                            click_title,
                                            click_snippet,
                                            query,
                                            intxn_num,
                                            cs.session_no)
                    Results_h[cs.session_no, intxn_num, click_rank] = result_object                 
                    clicks_a.append(result_object)
            
            #Interaction update
            for b in inter.clicked: 
                if (b):
                    click_rank = 0
                    click_count = len(b.click)
                    for click in b.click: 
                        click_rank += 1
                        rank    = int(click.rank.text)
                        clicks += str(rank) + '.'

                        #Attribute Holder of clicks
                        click_attr_h    = click.attrib 
                        start_time      = click_attr_h['starttime'] 
                        end_time        = click_attr_h['endtime'] 
                        total_click_time += timediff_ms(end_time, start_time)

                        #Update Result object
                        Results_h[cs.session_no, intxn_num, rank].clicked = True
                        Results_h[cs.session_no, intxn_num, rank].start_time = start_time
                        Results_h[cs.session_no, intxn_num, rank].end_time = end_time

                if (total_click_time > 0.0):
                    avg_click_time = total_click_time / click_count
            clicks = clicks.strip('.')
            ci = Interaction(interaction_start_time,
                                avg_click_time, 
                                click_count,
                                result_count,
                                clicks,
                                clicks_a)
            interactions_a.append(ci)
            Interactions_h[cs.session_no, intxn_num] = ci

        cs.interactions = interactions_a
        sessions_a.append(cs)
    return sessions_a
#End parseXML

#----------------------------------------------------------------------
#Main function
#----------------------------------------------------------------------
if __name__ == "__main__":
    xmlFileName = r'full_session.xml'

    sessions_a  = parseXML(xmlFileName)

    convert_to_csv(sessions_a)
    write_click_data()
    write_state_data()
