import numpy as np

def main():
    # python 3에서는 print() 으로 사용합니다.
    print("Main Function")
    num = 10
    print(f'num : {num}')
    lam = (60 * 1000.0) / num
    samples = np.random.poisson(lam, num)
    print(f'lam : {lam}')
    print(f'samples : {samples}')
    print(f'bug : {len(samples)}')

    np.save('possion_sample', samples)
    
if __name__ == "__main__":
	main()
