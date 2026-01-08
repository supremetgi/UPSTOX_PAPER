from colorama import Fore, Style, init

init()  # important for Windows

print(Fore.RED + "Red text")
print(Fore.GREEN + "Green text")
print(Fore.YELLOW + "Yellow text")
print(Style.BRIGHT + Fore.CYAN + "Bright cyan text")
print(Style.RESET_ALL)
