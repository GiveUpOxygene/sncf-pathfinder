import subprocess

def main():
    sentence_counts = [500, 1000, 5000, 10000, 20000]

    for count in sentence_counts:
        print(f"--- Testing with {count} sentences ---")
        command = f"python spacy_custom/test_efficiency.py {count}"
        try:
            subprocess.run(command, shell=True, check=True)
            print(f"--- Finished testing with {count} sentences ---\n")
        except subprocess.CalledProcessError as e:
            print(f"Error testing with {count} sentences: {e}")
            break

if __name__ == "__main__":
    main()
