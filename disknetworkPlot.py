import re
import sys
import math
import numpy as np
import matplotlib.pyplot as plt


class disknetwork:
    def __init__(self):

        f1='netbeforevm1.txt'
        f2='netbeforevm2.txt'
        f3='netbeforevm3.txt'
        f4='netbeforevm4.txt'

        g1='netaftervm1.txt'
        g2='netaftervm2.txt'
        g3='netaftervm3.txt'
        g4='netaftervm4.txt'

        h1='diskbeforevm1.txt'
        h2='diskbeforevm2.txt'
        h3='diskbeforevm3.txt'
        h4='diskbeforevm4.txt'

        t1='diskaftervm1.txt'
        t2='diskaftervm2.txt'
        t3='diskaftervm3.txt'
        t4='diskaftervm4.txt'

        list1 = []# for receive before mr
        list2 = []# for transmit before mr
        list3 = []# for receive after mr
        list4 = []# for transmit after mr

        list5 = []# for read before mr
        list6 = []# for write before mr
        list7 = []# for read after mr
        list8 = []# for write after mr

        list9 = []# for time taken to read before mr
        list10 = []# for time taken to write before mr
        list11 = []# for time taken to read after mr
        list12 = []# for time taken to write after mr

        iterator = 0
        with open(f1) as l1:
            for s in l1:
                try:
                    iterator+=1
                    x = re.findall("\d+", s)
                    if iterator == 4:
                        list1.append(int(float(x[1])))
                        list2.append(int(float(x[9])))
                except ValueError:
                    pass
        iterator = 0
        with open(f2) as l2:
            for s in l2:
                try:
                    iterator+=1
                    x = re.findall("\d+", s)
                    if iterator == 4:
                        list1.append(int(float(x[1])))
                        list2.append(int(float(x[9])))
                except ValueError:
                    pass
        iterator = 0
        with open(f3) as l3:
            for s in l3:
                try:
                    iterator+=1
                    x = re.findall("\d+", s)
                    if iterator == 4:
                        list1.append(int(float(x[1])))
                        list2.append(int(float(x[9])))
                except ValueError:
                    pass
        iterator = 0
        with open(f4) as l4:
            for s in l4:
                try:
                    iterator+=1
                    x = re.findall("\d+", s)
                    if iterator == 4:
                        list1.append(int(float(x[1])))
                        list2.append(int(float(x[9])))
                except ValueError:
                    pass

        iterator = 0
        with open(g1) as l1:
            for s in l1:
                try:
                    iterator+=1
                    x = re.findall("\d+", s)
                    if iterator == 4:
                        list3.append(int(float(x[1])))
                        list4.append(int(float(x[9])))
                except ValueError:
                    pass
        iterator = 0
        with open(g2) as l2:
            for s in l2:
                try:
                    iterator+=1
                    x = re.findall("\d+", s)
                    if iterator == 4:
                        list3.append(int(float(x[1])))
                        list4.append(int(float(x[9])))
                except ValueError:
                    pass
        iterator = 0
        with open(g3) as l3:
            for s in l3:
                try:
                    iterator+=1
                    x = re.findall("\d+", s)
                    if iterator == 4:
                        list3.append(int(float(x[1])))
                        list4.append(int(float(x[9])))
                except ValueError:
                    pass
        iterator = 0
        with open(g4) as l4:
            for s in l4:
                try:
                    iterator+=1
                    x = re.findall("\d+", s)
                    if iterator == 4:
                        list3.append(int(float(x[1])))
                        list4.append(int(float(x[9])))
                except ValueError:
                    pass


        with open(h1) as l1:
            for s in l1:
                try:
                    x = re.findall("sdc1", s)
                    if x:
                        x = re.findall("\d+", s)
                        list5.append(float(x[6]))
                        list6.append(float(x[10]))
                        list9.append(float(x[9]))
                        list10.append(float(x[13]))
                except ValueError:
                    pass

        with open(h2) as l2:
            for s in l2:
                try:
                    x = re.findall("sdc1", s)
                    if x:
                        x = re.findall("\d+", s)
                        list5.append(float(x[6]))
                        list6.append(float(x[10]))
                        list9.append(float(x[9]))
                        list10.append(float(x[13]))
                except ValueError:
                    pass

        with open(h3) as l3:
            for s in l3:
                try:
                    x = re.findall("sdc1", s)
                    if x:
                        x = re.findall("\d+", s)
                        list5.append(float(x[6]))
                        list6.append(float(x[10]))
                        list9.append(float(x[9]))
                        list10.append(float(x[13]))
                except ValueError:
                    pass

        with open(h4) as l4:
            for s in l4:
                try:
                    x = re.findall("sdc1", s)
                    if x:
                        x = re.findall("\d+", s)
                        list5.append(float(x[6]))
                        list6.append(float(x[10]))
                        list9.append(float(x[9]))
                        list10.append(float(x[13]))
                except ValueError:
                    pass


        with open(t1) as l1:
            for s in l1:
                try:
                    x = re.findall("sdc1", s)
                    if x:
                        x = re.findall("\d+", s)
                        list7.append(float(x[6]))
                        list8.append(float(x[10]))
                        list11.append(float(x[9]))
                        list12.append(float(x[13]))
                except ValueError:
                    pass

        with open(t2) as l2:
            for s in l2:
                try:
                    x = re.findall("sdc1", s)
                    if x:
                        x = re.findall("\d+", s)
                        list7.append(float(x[6]))
                        list8.append(float(x[10]))
                        list11.append(float(x[9]))
                        list12.append(float(x[13]))
                except ValueError:
                    pass

        with open(t3) as l3:
            for s in l3:
                try:
                    x = re.findall("sdc1", s)
                    if x:
                        x = re.findall("\d+", s)
                        list7.append(float(x[6]))
                        list8.append(float(x[10]))
                        list11.append(float(x[9]))
                        list12.append(float(x[13]))
                except ValueError:
                    pass

        with open(t4) as l4:
            for s in l4:
                try:
                    x = re.findall("sdc1", s)
                    if x:
                        x = re.findall("\d+", s)
                        list7.append(float(x[6]))
                        list8.append(float(x[10]))
                        list11.append(float(x[9]))
                        list12.append(float(x[13]))
                except ValueError:
                    pass


        listreceive = [x1 - x2 for (x1, x2) in zip(list3, list1)]
        listtransmit = [x1 - x2 for (x1, x2) in zip(list4, list2)]
        #print listreceive
        #print listtransmit

        print list5
        print list6
        print list7
        print list8

        list13 = [x1 - x2 for (x1, x2) in zip(list7, list5)]#read
        list14 = [x1 - x2 for (x1, x2) in zip(list8, list6)]#write
        list15 = [x1 - x2 for (x1, x2) in zip(list11, list9)]#read time
        list16 = [x1 - x2 for (x1, x2) in zip(list12, list10)]#write time

        listread = [x1*1000 / x2 for (x1, x2) in zip(list13, list15)]
        listwrite = [x1*10 / x2 for (x1, x2) in zip(list14, list16)]

        print listread
        print listwrite

        x1 = [1,2,3,4]
        fig1 = plt.figure()
        ax1 = fig1.add_subplot(221)
        ax1.set_title("Network Receive I/O")
        #ax1.set_xlabel("Slaves")
        ax1.set_ylabel("Bytes received")
        ax1.bar(x1, listreceive, width=0.2, color='y', align='center')


        ax1 = fig1.add_subplot(222)
        ax1.set_title("Network Transmit I/O")
        #ax1.set_xlabel("Slaves")
        ax1.set_ylabel("Bytes transmitted")
        ax1.bar(x1, listtransmit, width=0.2, color='g', align='center')


        ax1 = fig1.add_subplot(223)
        ax1.set_title("Disk Read I/O")
        ax1.set_xlabel("Slaves")
        ax1.set_ylabel("Bytes read in bytes/seconds")
        ax1.bar(x1, listread, width=0.2, color='r', align='center')

        ax1 = fig1.add_subplot(224)
        ax1.set_title("Disk Write I/O")
        ax1.set_xlabel("Slaves")
        ax1.set_ylabel("Bytes written in 100 bytes/seconds")
        ax1.bar(x1, listwrite, width=0.2, color='b', align='center')
        plt.show()

if __name__=='__main__':

        disknetwork()
