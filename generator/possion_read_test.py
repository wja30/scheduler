import numpy as np

def main():
    # python 3에서는 print() 으로 사용합니다.
    print("Main Function")

    samples = np.load('possion_sample.npy')
    #samples=np.load('possion_sample.npy')

    print(f'samples : {samples}')

if __name__ == "__main__":
	main()
