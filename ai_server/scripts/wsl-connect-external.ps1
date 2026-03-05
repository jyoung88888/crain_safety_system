# `start_wsl2_portforward.ps1` 파일
# $script = {
#     netsh interface portproxy add v4tov4 listenport=8088 listenaddress=0.0.0.0 connectport=8088 connectaddress=127.0.0.1
#     netsh advfirewall firewall add rule name="TCP/8088" protocol=TCP dir=in localport=8088 action=allow
    
#     netsh interface portproxy add v4tov4 listenport=8554 listenaddress=0.0.0.0 connectport=8554 connectaddress=127.0.0.1
#     netsh advfirewall firewall add rule name="TCP/8554" protocol=TCP dir=in localport=8554 action=allow

#     netsh interface portproxy add v4tov4 listenport=1935 listenaddress=0.0.0.0 connectport=1935 connectaddress=127.0.0.1
#     netsh advfirewall firewall add rule name="TCP/1935" protocol=TCP dir=in localport=1935 action=allow

#     netsh interface portproxy add v4tov4 listenport=8888 listenaddress=0.0.0.0 connectport=8888 connectaddress=127.0.0.1
#     netsh advfirewall firewall add rule name="TCP/8888" protocol=TCP dir=in localport=8888 action=allow

#     netsh interface portproxy add v4tov4 listenport=8889 listenaddress=0.0.0.0 connectport=8889 connectaddress=127.0.0.1
#     netsh advfirewall firewall add rule name="TCP/8889" protocol=TCP dir=in localport=8889 action=allow

#     netsh interface portproxy add v4tov4 listenport=8889 listenaddress=0.0.0.0 connectport=8889 connectaddress=127.0.0.1
#     netsh advfirewall firewall add rule name="TCP/8889" protocol=TCP dir=in localport=8889 action=allow

#     netsh interface portproxy add v4tov4 listenport=8890 listenaddress=0.0.0.0 connectport=8890 connectaddress=127.0.0.1
#     netsh advfirewall firewall add rule name="TCP/8890" protocol=TCP dir=in localport=8890 action=allow

#     netsh interface portproxy add v4tov4 listenport=8189 listenaddress=0.0.0.0 connectport=8189 connectaddress=127.0.0.1
#     netsh advfirewall firewall add rule name="TCP/8189" protocol=TCP dir=in localport=8189 action=allow
# }
# Start-Process -FilePath powershell.exe -ArgumentList "-Command & {$script run}" -verb RunAs



#기존포트포워딩 삭제
netsh interface portproxy delete v4tov4 listenport=8088 listenaddress=0.0.0.0
netsh interface portproxy delete v4tov4 listenport=8554 listenaddress=0.0.0.0
netsh interface portproxy delete v4tov4 listenport=8000 listenaddress=0.0.0.0
netsh interface portproxy delete v4tov4 listenport=8001 listenaddress=0.0.0.0
netsh interface portproxy delete v4tov4 listenport=1935 listenaddress=0.0.0.0
netsh interface portproxy delete v4tov4 listenport=8888 listenaddress=0.0.0.0
netsh interface portproxy delete v4tov4 listenport=8889 listenaddress=0.0.0.0
netsh interface portproxy delete v4tov4 listenport=8890 listenaddress=0.0.0.0
netsh interface portproxy delete v4tov4 listenport=8189 listenaddress=0.0.0.0

#재부팅되어 리셋된 wsl ip를 변수에 할당
$wsl_ip = wsl hostname -I | Out-String
$wsl_ip = $wsl_ip.Split(' ')[0]
$wsl_ip = $wsl_ip.Trim()

#새로운 ip로 포트포워딩
netsh interface portproxy add v4tov4 listenport=8088 listenaddress=0.0.0.0 connectport=8088 connectaddress=$wsl_ip
netsh interface portproxy add v4tov4 listenport=8554 listenaddress=0.0.0.0 connectport=8554 connectaddress=$wsl_ip
netsh interface portproxy add v4tov4 listenport=8000 listenaddress=0.0.0.0 connectport=8000 connectaddress=$wsl_ip
netsh interface portproxy add v4tov4 listenport=8001 listenaddress=0.0.0.0 connectport=8001 connectaddress=$wsl_ip
netsh interface portproxy add v4tov4 listenport=1935 listenaddress=0.0.0.0 connectport=1935 connectaddress=$wsl_ip
netsh interface portproxy add v4tov4 listenport=8888 listenaddress=0.0.0.0 connectport=8888 connectaddress=$wsl_ip
netsh interface portproxy add v4tov4 listenport=8889 listenaddress=0.0.0.0 connectport=8889 connectaddress=$wsl_ip
netsh interface portproxy add v4tov4 listenport=8890 listenaddress=0.0.0.0 connectport=8890 connectaddress=$wsl_ip
netsh interface portproxy add v4tov4 listenport=8189 listenaddress=0.0.0.0 connectport=8189 connectaddress=$wsl_ip