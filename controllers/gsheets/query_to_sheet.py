import pandas as pd

df = pd.DataFrame()
print("a")

import csv
with open('eggs.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in spamreader:
        print(', '.join(row))




with open('requirements.txt', 'r') as f:
    data = f.read()


def asteriscos(n:int):
    for i in range(n+1):
        print(i*"*")

asteriscos(4)


zero = [
    ['  0000    '],
    ['   000    '],
    ['    00    '],
    ['     0    '],
    ['     0    '],
    ['    00    '],
    ['   000    '],
    ['  0000    ']
]

for i in zero:
   print(str(i[0]))


#Original:
import collections
ranks = [str(n) for n in range(2,11)] + list("JQKA")
suits = 'spades, diamonds, clubs, hearts'.split(", ")
Card = collections.namedtuple("card", ["rank", "suite"])
[{"rank": rank, "suit": suit} for suit in suits for rank in ranks]

#Simplificado:
lista = []
for suit in suits:
    for rank in ranks:
        lista.append({"rank": rank, "suit": suit})

#todas las combinaciones:
print(lista)

#cantidad de cartas:
print(len(lista))

#cantidad de cartas por palo:
print(len([x for x in lista if x['suit'] == "hearts"]))

#lista de cartas por palo:
[x for x in lista if x['suit'] == "hearts"]

#lista de cartas por rank:
[x for x in lista if x['rank'] == "J"]

#todas las combinaciones con orden invertido:
[{"rank": rank, "suit": suit} for suit in suits for rank in reversed(ranks)]

#lista de clubs son slice notation:
#como cada palo tiene 13 y clubs esta tercero, tenemos que contar desde 26 hasta 26+13
lista[26:(26+13)]