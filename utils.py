import subprocess

class Command(object):
    def __init__(self, text):
        self.text = text

    def execute(self):
        self.proc = subprocess.Popen(self.text, shell=True)
        self.proc.wait()

class CommandSequence(Command):
    def __init__(self, *steps):
        self.steps = steps

    def execute(self):
        for step in self.steps:
            step.execute()

# Example usage:
if __name__ == "__main__":
    cmd1 = Command("echo Hello, World!")
    cmd2 = Command("ls -l")
    sequence = CommandSequence(cmd1, cmd2)
    sequence.execute()