import timeformatter as tf
from blessings import Terminal


def print_header(thread_id):
    global term, space
    xoffset = 0
    yoffset = thread_id*10

    if(thread_id >= 5):
        xoffset = 70
        yoffset = (thread_id-5)*10
    if(thread_id >= 10):
        xoffset = 140
        yoffset = (thread_id-10)*10

    print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset)+term.white + term.on_bright_black +
          f"  Thread {thread_id}  " + term.normal)


def print_current_user_check_bar(thread_id, user):
    global term, space
    xoffset = 4
    yoffset = thread_id*10+2

    if(thread_id >= 5):
        xoffset = 74
        yoffset = (thread_id-5)*10+2
    if(thread_id >= 10):
        xoffset = 144
        yoffset = (thread_id-10)*10+2

    print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
          f"{term.bright_black}Currently checking user - {term.white} {term.bold}{user.name}{term.normal}")


def print_status_bar(thread_id, msg, type):
    global term, space
    xoffset = 4
    yoffset = thread_id*10+3

    if(thread_id >= 5):
        xoffset = 74
        yoffset = (thread_id-5)*10+3
    if(thread_id >= 10):
        xoffset = 144
        yoffset = (thread_id-10)*10+3

    if(type == 'success'):
        print(term.move(yoffset, xoffset)+space + term.move(yoffset, xoffset) + term.bright_black+"Status: "+term.green +
              msg + term.normal)
    elif(type == 'error'):
        print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) + term.bright_black+"Status: "+term.red +
              msg + term.normal)


def print_pogress_bar(thread_id, user_index, batch_size):
    global term, space
    xoffset = 4
    yoffset = thread_id*10+4

    if(thread_id >= 5):
        xoffset = 74
        yoffset = (thread_id-5)*10+4
    if(thread_id >= 10):
        xoffset = 144
        yoffset = (thread_id-10)*10+4

    global term
    print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
          f"{term.bright_black}Checked users:\t{term.cyan}{user_index}/{batch_size}{term.bright_black}." + term.normal)


def print_estimated_time_bar(thread_id, user_index, batch_size, avg_requests, reqs_per_min):
    global term, space
    xoffset = 4
    yoffset = thread_id*10+5

    if(thread_id >= 5):
        xoffset = 74
        yoffset = (thread_id-5)*10+5
    if(thread_id >= 10):
        xoffset = 144
        yoffset = (thread_id-10)*10+5

    remaining_time_minutes = (batch_size - user_index) * \
        (avg_requests/200)/reqs_per_min
    if(remaining_time_minutes >= 60):
        time_string = f"{remaining_time_minutes//60} h. {remaining_time_minutes%60} min."
    else:
        time_string = f"{remaining_time_minutes} min."

    print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
          f"{term.bright_black}Estimated time:\t{term.cyan}{time_string}" + term.normal)


def init_pretty_print():
    global term, space
    term = Terminal()
    space = 69*' '
    print(term.enter_fullscreen)
