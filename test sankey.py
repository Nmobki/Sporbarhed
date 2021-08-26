
import networkx as nx
import pandas as pd

df_ordrer = pd.DataFrame(columns=['Ordre','Ordretype','Relateret ordre','Relateret ordretype'])
df_ordrer['Ordre'] = ['040627','040839','040839','040791','040791','040791',
                      '040791','040847','040847','040847']
df_ordrer['Ordretype'] = ['Færdigkaffe','Formalet kaffe','Formalet kaffe','Ristet kaffe',
                          'Ristet kaffe','Ristet kaffe','Ristet kaffe','Ristet kaffe','Ristet kaffe','Ristet kaffe']
df_ordrer['Relateret ordre'] = ['040839','040791','040847','20-226/2','20-227/1',
                                '20-219/1','21-028H/1','20-227/1','20-219/1','21-028H/1']
df_ordrer['Relateret ordretype'] = ['Formalet kaffe','Ristet kaffe','Ristet kaffe','Råkaffe',
                                    'Råkaffe','Råkaffe','Råkaffe','Råkaffe','Råkaffe','Råkaffe']

df_ordrer['Primær'] = df_ordrer['Ordretype'] + '\n' + df_ordrer['Ordre'] 
df_ordrer['Sekundær'] = df_ordrer['Relateret ordretype'] + '\n' + df_ordrer['Relateret ordre']

array_for_drawing = list(df_ordrer[['Primær','Sekundær']].itertuples(index=False, name=None))

g = nx.DiGraph()
g.add_edges_from(array_for_drawing)


p = nx.drawing.nx_pydot.to_pydot(g)
p.write_png(r'\\filsrv01\BKI\11. Økonomi\04 - Controlling\NMO\4. Kvalitet\Sporbarhedstest\Tests via PowerApps\test.png')
