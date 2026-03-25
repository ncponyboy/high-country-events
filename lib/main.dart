import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'services/event_service.dart';
import 'services/specials_service.dart';
import 'services/user_preferences_service.dart';
import 'screens/events_screen.dart';
import 'screens/specials_screen.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const HighCountryEventsApp());
}

class HighCountryEventsApp extends StatelessWidget {
  const HighCountryEventsApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MultiProvider(
      providers: [
        ChangeNotifierProvider(create: (_) => EventService()),
        ChangeNotifierProvider(create: (_) => SpecialsService()),
        ChangeNotifierProvider(create: (_) => UserPreferencesService()),
      ],
      child: MaterialApp(
        title: 'High Country Events',
        debugShowCheckedModeBanner: false,
        theme: _buildTheme(),
        home: const _AppStartup(),
      ),
    );
  }

  ThemeData _buildTheme() {
    const Color primary = Color(0xFF1A237E);
    const Color accent = Color(0xFF283593);

    return ThemeData(
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(
        seedColor: primary,
        primary: primary,
        secondary: accent,
        brightness: Brightness.light,
      ),
      appBarTheme: const AppBarTheme(
        backgroundColor: primary,
        foregroundColor: Colors.white,
        elevation: 0,
      ),
      bottomNavigationBarTheme: const BottomNavigationBarThemeData(
        selectedItemColor: primary,
        unselectedItemColor: Colors.grey,
        backgroundColor: Colors.white,
        type: BottomNavigationBarType.fixed,
        elevation: 8,
      ),
      scaffoldBackgroundColor: Color(0xFFF5F5F5),
    );
  }
}

// ---------------------------------------------------------------------------
// Startup: fetch data then show main shell
// ---------------------------------------------------------------------------

class _AppStartup extends StatefulWidget {
  const _AppStartup();

  @override
  State<_AppStartup> createState() => _AppStartupState();
}

class _AppStartupState extends State<_AppStartup> {
  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addPostFrameCallback((_) => _init());
  }

  Future<void> _init() async {
    final eventSvc = context.read<EventService>();
    final specialsSvc = context.read<SpecialsService>();
    await Future.wait([
      eventSvc.fetchEvents(),
      specialsSvc.fetchSpecials(),
    ]);
  }

  @override
  Widget build(BuildContext context) {
    return const MainShell();
  }
}

// ---------------------------------------------------------------------------
// Main shell with bottom nav: Events | Specials
// ---------------------------------------------------------------------------

class MainShell extends StatefulWidget {
  const MainShell({super.key});

  @override
  State<MainShell> createState() => _MainShellState();
}

class _MainShellState extends State<MainShell> {
  int _currentIndex = 0;

  static const List<Widget> _screens = [
    EventsScreen(),
    SpecialsScreen(),
  ];

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: IndexedStack(
        index: _currentIndex,
        children: _screens,
      ),
      bottomNavigationBar: BottomNavigationBar(
        currentIndex: _currentIndex,
        onTap: (i) => setState(() => _currentIndex = i),
        selectedItemColor: const Color(0xFF1A237E),
        unselectedItemColor: Colors.grey.shade500,
        backgroundColor: Colors.white,
        elevation: 8,
        items: const [
          BottomNavigationBarItem(
            icon: Icon(Icons.event_outlined),
            activeIcon: Icon(Icons.event),
            label: 'Events',
          ),
          BottomNavigationBarItem(
            icon: Icon(Icons.local_offer_outlined),
            activeIcon: Icon(Icons.local_offer),
            label: 'Specials',
          ),
        ],
      ),
    );
  }
}
