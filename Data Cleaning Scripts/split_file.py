import numpy as np 
import pandas as pd
download_links_np = np.genfromtxt("download_links.txt", dtype=str, delimiter=',')

file1 = pd.DataFrame(download_links_np[:20000])
file2 = pd.DataFrame(download_links_np[20001:45000])
file3 = pd.DataFrame(download_links_np[45001:66500])

file1.to_csv("Links_1.csv")
file2.to_csv("Links_2.csv")
file3.to_csv("Links_3.csv")




# print file1[1:6], file2[1:6], file3[1:6]

# np.savetxt("Links_1.txt",file1)
# np.savetxt("Links_2.txt",file2)
# np.savetxt("Links_3.txt",file3)